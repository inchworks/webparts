// Copyright © Rob Burke inchworks.com, 2020.

package server

// Block HTTP requests according to the geographical location of the IP address.

import (
	"context"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/maxminddb-golang"
)

// put context key in its own type, to avoid collision with other packages using request context
type contextKey int

const contextKeyLocation = contextKey(0)

type location struct {
	country    string
	registered string
	ip         string
}

// GeoBlocker holds the parameters and state for geo-blocking. Typically only one is needed.
type GeoBlocker struct {
	Allow        bool // permit only specified countries, instead of blocking them
	ErrorLog     *log.Logger
	Reporter     func(r *http.Request, location string, ip net.IP) string
	ReportSingle bool   // report just location or registered country, not both
	Store        string // storage location for database

	file   string          // source file for database
	listed map[string]bool // specified countries

	// geoBlocking database
	mutex sync.RWMutex
	db    *maxminddb.Reader

	chDone chan bool
}

// Start initialises the geo-blocker.
func (gb *GeoBlocker) Start(countries []string) {

	// blocked countries
	gb.listed = make(map[string]bool)
	for _, c := range countries {
		gb.listed[strings.ToUpper(c)] = true
	}

	// reload geo database regularly
	gb.file = filepath.Join(gb.Store, "GeoLite2-Country.mmdb")
	gb.chDone = make(chan bool, 1)

	go gb.reloader(24*time.Hour, gb.chDone)
}

// GeoBlock initialises and returns a handler to block IPs for some locations.
func (gb *GeoBlocker) GeoBlock(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		var ip net.IP
		var ctry, reg string
		var blocked bool

		// location of request
		// ## We only need the parsed IP address for Reporter, and typically callers don't want it.
		ipStr, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			ctry, reg, ip = gb.Locate(ipStr)
		}

		// save for threat reporting
		ctx := context.WithValue(
			r.Context(),
			contextKeyLocation,
			location{country: ctry, registered: reg, ip: ipStr})

		// blocked location?
		listed := gb.listed[ctry] || gb.listed[reg]
		blocked = (listed == !gb.Allow) // blacklist or whitelist?

		if blocked {
			var loc, msg string
			if gb.ReportSingle {
				// simplify stats by showing just the location that caused blocking
				// (and show country if not whitelisted)
				if gb.listed[reg] {
					loc = reg
				} else {
					loc = ctry
				}
			} else {
				loc = location2(reg, ctry)
			}

			// report blocking
			if gb.Reporter != nil {
				msg = gb.Reporter(r, loc, ip)
			}

			// default message
			if msg == "" {
				msg = "Access from " + loc + " not allowed"
			}

			http.Error(w, msg, http.StatusForbidden)
		} else {

			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
	})
}

// Country returns the location country code for the current request.
func Country(r *http.Request) (loc string) {
	v := r.Context().Value(contextKeyLocation)
	if v != nil {
		loc = v.(location).country
	}
	return
}

// Locate looks up a remote address in the geolocation database, and returns the countries of origin and registration.
func (gb *GeoBlocker) Locate(ipStr string) (country, registered string, ip net.IP) {

	// lock database against reload
	gb.mutex.RLock()
	defer gb.mutex.RUnlock()

	if gb.db != nil {
		ip = net.ParseIP(ipStr)
		if ip != nil {

			// get location for IP address
			var geo struct {
				Country struct {
					ISOCode string `maxminddb:"iso_code"`
				} `maxminddb:"country"`
				RegisteredCountry struct {
					ISOCode string `maxminddb:"iso_code"`
				} `maxminddb:"registered_country"`
			}

			// lookup country code for IP address, and see if it is listed
			err := gb.db.Lookup(ip, &geo)
			if err != nil && gb.ErrorLog != nil {
				gb.ErrorLog.Print("Geo-location lookup:", err)
			} else {
				country = geo.Country.ISOCode
				registered = geo.RegisteredCountry.ISOCode
			}
		}
	}
	return
}

// Location returns both the registered and location country codes for the current request, if they are different.
func Location(r *http.Request) (loc string) {
	v := r.Context().Value(contextKeyLocation)
	if v != nil {
		locs := v.(location)
		loc = location2(locs.country, locs.registered)
	}
	return
}

// Registered returns the registered country codes for the current request.
func Registered(r *http.Request) (loc string) {
	v := r.Context().Value(contextKeyLocation)
	if v != nil {
		loc = v.(location).registered
	}
	return
}

// RemoteIP returns the originating IP address for the current request.
func RemoteIP(r *http.Request) (ip string) {
	v := r.Context().Value(contextKeyLocation)
	if v != nil {
		ip = v.(location).ip // cached
	} else {
		ip, _, _= net.SplitHostPort(r.RemoteAddr) // when Geo Block not used
	}
	return
}

// Stop ends geo-blocking.
func (gb *GeoBlocker) Stop() {

	// terminate the reloader, which closes the database
	close(gb.chDone)
}

// location2 returns both the registered and country codes for the current request, if they are different.
func location2(reg string, ctry string) string {

	if reg == ctry {
		return reg
	} else {
		return reg + ">" + ctry
	}
}

// reloadGeoDB closes the geo-location database and reopens the latest one.
func (gb *GeoBlocker) reloadGeoDB() {

	var err error

	// lock database usage during reload
	gb.mutex.Lock()
	defer gb.mutex.Unlock()

	// close in-use database
	if gb.db != nil {
		err = gb.db.Close()
		gb.db = nil
	}
	if err != nil && gb.ErrorLog != nil {
		gb.ErrorLog.Print("Closing geo-location database:", err)
	}

	// reopen latest one, if geo-blocking is specified
	if len(gb.listed) > 0 {
		gb.db, err = maxminddb.Open(gb.file)
		if err != nil && gb.ErrorLog != nil {
			gb.ErrorLog.Print("No geo-location database:", err) // continue operation without geo-blocking
		}
	}
}

// reloader performs periodic updates.
func (gb *GeoBlocker) reloader(d time.Duration, done <-chan bool) {

	// first load
	gb.reloadGeoDB()

	// daily reload
	t := time.NewTicker(d)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			gb.reloadGeoDB()

		case <-done:
			if gb.db != nil {
				gb.mutex.Lock()
				gb.db.Close()
				gb.db = nil
				gb.mutex.Unlock()
			}
			return
		}
	}
}
