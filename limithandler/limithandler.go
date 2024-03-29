// Copyright © Rob Burke inchworks.com, 2020.

// Package limithandler implements a rate limiter for HTTP requests.
//
// It is based on https://www.alexedwards.net/blog/how-to-rate-limit-http-requests,
// with an interface model copied loosely from https://github.com/justinas/nosurf.
package limithandler

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	escalate  = 3 // ban escalation per level (multiply by 1<<escalate = 8)
	precision = 8 // precision of timing
)

type Handler struct {
	limit *limiter

	// handlers wrapped
	banned    http.Handler
	failure   http.Handler
	ignored   http.Handler
	report    func(*http.Request, string, string)
	reportAll bool
	success   http.Handler
}

type Handlers struct {
	banFor      time.Duration
	forget      time.Duration
	visitorAddr func(*http.Request) string

	limiters map[string]*limiter
	release  *time.Ticker
	chDone   <-chan bool
}

type limiter struct {
	lhs *Handlers

	// parameters
	rate     rate.Limit // max. requests per second
	burst    int        // allowed burst
	banAfter int        // rejects until banned
	alsoBan  []string   // extend ban to these limits

	// internal data
	mu       sync.Mutex
	visitors map[string]*visitor
	rejects  int		// rejected requests (statistic)
}

// rate limiter for each visitor
type visitor struct {
	lastSeen time.Time
	limiter  *rate.Limiter
	reject   bool
	rejects  int
	banTo    time.Time
	banLevel int // -1 = not banned
}

// Allow checks the client's HTTP request rate against a limit. If rejected, it returns a suggested status code.
// Use it to implement an HTTP request handler that does additional processing, or to limit rates on client errors.
// If only rate limiting is needed, use ServeHTTP instead.
func (lh *Handler) Allow(r *http.Request) (ok bool, status int) {

	lim := lh.limit
	lhs := lim.lhs

	lim.mu.Lock()
	defer lim.mu.Unlock()

	// visitor address
	ip, _, err := net.SplitHostPort(lhs.visitorAddr(r))
	if err != nil {
		log.Println(err.Error())
		ok = true // safer not to block access
		return
	}

	// limiter for this limit and visitor
	v := lim.visitor(ip)
	if !v.banTo.IsZero() || (v.limiter != nil && !v.limiter.Allow()) || v.reject {

		// count rejections and report first one
		status = lh.reject(r, ip, v)
		return
	}

	ok = true
	return
}

// New returns a Handler for a specified rate limit.
// If called multiple times for the same limit name, by justinas/alice for example, it will return the same item each time.
// Specify alsoBan to extend a ban to other limits. Typically this might be a single escalating limiter that bans all requests.
// If alsoBan specifies this limit (alsoBan==limit), the duration of a repeated ban will increase exponentially.
// Note that escalating bans probably doesn't increase security but it serves to reduce the number of log entries for miscreants.
// The parameter next may be nil if Allow() and not ServeHTTP() is to be called.
func (lhs *Handlers) New(limit string, every time.Duration, burst int, banAfter int, alsoBan string, next http.Handler) *Handler {

	lim := lhs.limiters[limit]
	if lim == nil {
		lim = &limiter{
			lhs:      lhs,
			rate:     rate.Every(every),
			burst:    burst,
			banAfter: banAfter,
			alsoBan:  strings.Split(alsoBan, ","),
			visitors: make(map[string]*visitor),
		}
		lhs.limiters[limit] = lim
	}
	return &Handler{
		limit:   lim,
		banned:  http.HandlerFunc(defaultBannedHandler),
		failure: http.HandlerFunc(defaultFailureHandler),
		ignored: http.HandlerFunc(defaultIgnoredHandler),
		success: next,
	}
}

// NewUnlimited returns a Handler with no rate limit. Its purpose is to implement an extended ban on a wider set of events.
// If alsoBan specifies this limit (alsoBan==limit), the duration of a repeated ban will increase exponentially.
func (lhs *Handlers) NewUnlimited(limit string, alsoBan string, next http.Handler) *Handler {

	lim := lhs.limiters[limit]
	if lim == nil {
		lim = &limiter{
			lhs:      lhs,
			alsoBan:  strings.Split(alsoBan, ","),
			visitors: make(map[string]*visitor),
		}
		lhs.limiters[limit] = lim
	}
	return &Handler{
		limit:   lim,
		banned:  http.HandlerFunc(defaultBannedHandler),
		failure: http.HandlerFunc(defaultFailureHandler),
		ignored: http.HandlerFunc(defaultIgnoredHandler),
		success: next,
	}
}

// RejectsCounted returns a statistic of the total number of requests rejected, and resets the count.
func (lhs *Handlers) RejectsCounted() (rejects int) {

	for _, lim := range lhs.limiters {
		lim.mu.Lock()
		rejects += lim.rejects
		lim.rejects = 0
		lim.mu.Unlock()
	}
	return
}

// ServeHTTP implements an HTTP request handler to checks a client's request rate.
// If the rate is acceptable, the specified next handler is caller.
func (lh *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ok, status := lh.Allow(r)
	if ok {
		lh.success.ServeHTTP(w, r)

	} else {
		switch status {
		case http.StatusForbidden:
			lh.banned.ServeHTTP(w, r) // newly banned

		case http.StatusNotFound:
			lh.ignored.ServeHTTP(w, r) // banned and ignored

		case http.StatusTooManyRequests:
			fallthrough

		default:
			lh.failure.ServeHTTP(w, r) // limit exceeded
		}
	}
}

// SetBannedHandler specifies a function to be called when requester has been banned.
// (Deprecated in favour of SetBanHandlers.)
func (lh *Handler) SetBannedHandler(handler http.Handler) {
	lh.banned = handler
	lh.ignored = handler
}

// SetFailureHandler specifies a function to be called when the rate limit is exceeded.
func (lh *Handler) SetFailureHandler(handler http.Handler) {
	lh.failure = handler
}

// SetBanHandlers specifies functions to be called when requester is first banned, and on subsequent requests.
func (lh *Handler) SetBanHandlers(banned, ignored http.Handler) {
	lh.banned = banned
	lh.ignored = ignored
}

// SetReportHandler specifies a function for reporting significant activity to the application.
func (lh *Handler) SetReportHandler(handler func(r *http.Request, ip string, status string)) {
	lh.report = handler
	lh.reportAll = false
}

// SetReportAllHandler specifies a function for reporting all activity to the application.
func (lh *Handler) SetReportAllHandler(handler func(r *http.Request, ip string, status string)) {
	lh.report = handler
	lh.reportAll = true
}

// SetVisitorAddr specifies a function to extract a visitor's IP address from a request.
// The default is to use Request.RemoteAddr.
// Alternatives of "x-real-ip" or "x-forwarded-for" from the Request.Header are needed if the server is behind a load balancer or other proxy.
// (Take care - clients can spoof request headers, so use information only from trusted proxies.
// See https://stackoverflow.com/questions/3003145/how-to-get-the-client-ip-address-in-php/55790#55790.)

func (lhs *Handlers) SetVisitorAddr(fn func(*http.Request) string) {
	lhs.visitorAddr = fn
}

// Start returns a set of limitHandlers. Typically only one set is needed.
func Start(ban time.Duration, forget time.Duration) *Handlers {

	var tick time.Duration
	if ban < forget {
		tick = ban / precision
	} else {
		tick = forget / precision
	}

	lhs := &Handlers{
		banFor:      ban,
		forget:      forget,
		visitorAddr: defaultVisitorAddr,

		limiters: make(map[string]*limiter),
		release:  time.NewTicker(tick),
	}

	// start background goroutine to remove old entries from the visitors map
	go lhs.worker()

	return lhs
}

// Stop terminates LimitHander operation.
func (lhs *Handlers) Stop() {
	lhs.release.Stop()
}

// ban blocks a misbehaving visitor
func (lim *limiter) ban(ip string, v *visitor) {

	for _, l := range lim.alsoBan {
		lim1 := lim.lhs.limiters[l]

		if lim1 == lim {
			// escalate the ban following previous bans
			v.banLevel++

		} else if lim1 != nil {
			// extend ban to another limit
			lim1.visitor(ip).reject = true
		}
	}

	// time when ban will end, with escalation
	if v.banLevel == -1 {
		v.banLevel = 0
	}
	v.banTo = time.Now().Add(lim.lhs.banFor << (v.banLevel * escalate))
}

// defaultBannedHandler calls an HTTP error for a newly banned IP address.
func defaultBannedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Banned for suspected intrusion attempt", http.StatusForbidden)
}

// defaultFailureHandler calls an HTTP error for limit failures.
func defaultFailureHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
}

// defaultIgnoredHandler calls an HTTP error for an already banned IP address.
func defaultIgnoredHandler(w http.ResponseWriter, r *http.Request) {

	// trying a different strategy in the faint hope that idiot bots might give up sooner
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

// defaultVisitorAddr returns the IP address of a visitor, from Request.RemoteAddr.
func defaultVisitorAddr(r *http.Request) string {
	return r.RemoteAddr
}

// reject records a rate rejection for a visitor, and returns a status for reporting.
// Note that in reporting we distinguish between extended bans, called "banned", and single limit bans, called "blocked".
func (lh *Handler) reject(r *http.Request, ip string, v *visitor) int {

	var httpStatus int
	var limitStatus string
	lim := lh.limit

	// count rejections
	v.rejects++
	lim.rejects++

	if v.reject {

		// extended ban requested
		v.reject = false
		lim.ban(ip, v)
		limitStatus = fmt.Sprint("banned at level ", v.banLevel)
		httpStatus = http.StatusForbidden

	} else if v.rejects == lim.banAfter {

		// ban threshold reached for first time
		lim.ban(ip, v)
		limitStatus = fmt.Sprint("blocked at level ", v.banLevel)
		httpStatus = http.StatusForbidden

	} else if v.rejects > lim.banAfter {

		// already banned - send a less helpful response
		httpStatus = http.StatusNotFound

	} else if v.rejects == 1 {

		// rate limit reached for first time
		limitStatus = "rejected"
		httpStatus = http.StatusTooManyRequests

	} else {

		// already rate limited
		httpStatus = http.StatusTooManyRequests
	}

	if lh.report != nil {
		if limitStatus != "" || lh.reportAll {
			lh.report(r, ip, limitStatus)
		}
	}

	return httpStatus
}

// visitor returns visitor data, including a rate limiter.
func (lim *limiter) visitor(id string) *visitor {
	v, exists := lim.visitors[id]
	if !exists {

		// rate limiter for new visitor
		if lim.rate != 0 {
			limiter := rate.NewLimiter(lim.rate, lim.burst)
			v = &visitor{limiter: limiter, lastSeen: time.Now(), banLevel: -1}
		} else {
			v = &visitor{lastSeen: time.Now(), banLevel: -1}
		}
		lim.visitors[id] = v

	} else {
		// last seen time for the visitor
		v.lastSeen = time.Now()
	}

	return v
}

// worker goroutine checks the maps for visitors that can be un-banned or forgotten.
func (lhs *Handlers) worker() {

	for {
		select {
		case <-lhs.release.C:

			for _, lim := range lhs.limiters {
				lim.mu.Lock()

				for id, v := range lim.visitors {

					if v.banLevel < 0 {
						// forget old good visitors quickly
						if time.Since(v.lastSeen) > lhs.forget {
							delete(lim.visitors, id)
						}

					} else if v.banTo.IsZero() {
						// remember bad visitors for longer
						forget := lhs.forget << ((v.banLevel + 1) * escalate)
						if time.Since(v.lastSeen) > forget {
							delete(lim.visitors, id)
						}

					} else if time.Since(v.banTo) > 0 {
						// lift ban
						v.banTo = time.Time{}
						v.rejects = 0
					}
				}
				lim.mu.Unlock()
			}

		case <-lhs.chDone:
			return
		}
	}
}
