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
	banned  http.Handler
	failure http.Handler
	report  func(*http.Request, string, string)
	success http.Handler
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
}

// rate limiter for each visitor
type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
	rejects  int
	banTo    time.Time
	banLevel int
}

// Allow checks the client's HTTP request rate against a limit. If rejected, it returns a suggested status code.
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
	if v.rejects > lim.banAfter || !v.banTo.IsZero() || v.limiter.Allow() == false {

		// count rejections and report first one
		status = lh.reject(r, ip, v)
		return
	}

	ok = true
	return
}

// New returns a LimitHandler for a specified rate limit.
// If called multiple times for the same limit, by justinas/alice for example, it will return the same item each time.
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
		report:  defaultReportHandler,
		success: next,
	}
}

// ServeHTTP implements an HTTP request handler to checks a client's request rate.
// If the rate is acceptable, the specified next handler is caller.
func (lh *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ok, status := lh.Allow(r)
	if ok {
		lh.success.ServeHTTP(w, r)

	} else if status == http.StatusForbidden {
		lh.banned.ServeHTTP(w, r) // banned

	} else {
		lh.failure.ServeHTTP(w, r) // limit exceeded
	}
}

// SetBannedHandler specifies a function to be called when requester has been banned.
func (lh *Handler) SetBannedHandler(handler http.Handler) {
	lh.banned = handler
}

// SetFailureHandler specifies a function to be called when the rate limit is exceeded.
func (lh *Handler) SetFailureHandler(handler http.Handler) {
	lh.failure = handler
}

// SetReportHandler specifies a function for reporting activity to the application.
func (lh *Handler) SetReportHandler(handler func(r *http.Request, ip string, status string)) {
	lh.report = handler
}

// SetVisitorAddr specifies a function for reporting activity to the application.
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
func (lim *limiter) ban(r *http.Request, ip string, v *visitor) {

	// escalate the ban following previous bans
	v.banLevel++
	v.banTo = time.Now().Add(lim.lhs.banFor << (v.banLevel * escalate))

	// extend ban to other limits
	for _, l := range lim.alsoBan {
		lim1 := lim.lhs.limiters[l]

		if lim1 != nil {
			v1 := lim1.visitor(ip)
			v1.banTo = v.banTo
			v1.banLevel = v.banLevel
			v1.rejects = 0
		}
	}
}

// defaultBannedHandler calls an HTTP error for a banned IP address.
func defaultBannedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Banned for suspected intrusion attempt", http.StatusForbidden)
}

// defaultFailureHandler calls an HTTP error for limit failures.
func defaultFailureHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
}

// defaultReportHandler is a null handler for rejections.
func defaultReportHandler(*http.Request, string, string) {}

// defaultVisitorAddr returns the IP address of a visitor, from Request.RemoteAddr.
// Alternatives of "x-real-ip" or "x-forwarded-for" from the Request.Header are needed if the server is behind a load balancer or other proxy.
// Take care - clients can spoof x-forwarded-for when there is no proxy server.
func defaultVisitorAddr(r *http.Request) string {
	return r.RemoteAddr
}

// reject records a rate rejection for a visitor, and returns a status for reporting.
func (lh *Handler) reject(r *http.Request, ip string, v *visitor) int {

	lim := lh.limit

	// count rejections
	v.rejects++

	// check for added ban first, because the limit could be 1
	if !v.banTo.IsZero() {
		if v.rejects == 1 {
			lh.report(r, ip, "also banned")
		}
		return http.StatusForbidden
	}

	// next check for first ban
	if v.rejects == lim.banAfter {

		// ban threshold reached for first time
		lim.ban(r, ip, v)
		lh.report(r, ip, fmt.Sprint("banned at level ", v.banLevel))
		return http.StatusForbidden

	} else if v.rejects == 1 {

		// limit reached for first time
		lh.report(r, ip, "rejected")
	}
	return http.StatusTooManyRequests
}

// worker goroutine checks the maps for visitors that can be un-banned or forgotten.
func (lhs *Handlers) worker() {

	for {
		select {
		case <-lhs.release.C:

			for _, lim := range lhs.limiters {
				lim.mu.Lock()

				for id, v := range lim.visitors {

					if v.banLevel == 0 {
						// forget old good visitors quickly
						if time.Since(v.lastSeen) > lhs.forget {
							delete(lim.visitors, id)
						}

					} else if v.banTo.IsZero() {
						// remember bad visitors for longer - twice their last ban
						forget := lhs.banFor << (v.banLevel*escalate + 1)
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

// visitor returns visitor data, including a rate limiter.
func (lim *limiter) visitor(id string) *visitor {
	v, exists := lim.visitors[id]
	if !exists {

		// rate limiter for new visitor
		limiter := rate.NewLimiter(lim.rate, lim.burst)
		v = &visitor{limiter: limiter, lastSeen: time.Now()}
		lim.visitors[id] = v

	} else {
		// last seen time for the visitor
		v.lastSeen = time.Now()
	}
	return v
}
