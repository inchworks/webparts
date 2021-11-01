// Copyright © Rob Burke inchworks.com, 2020.

// Package server implements an HTTPS web server.
// The configuration is idiosyncratic, and not intended to be suitable for everyone.
package server

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"time"

	"golang.org/x/crypto/acme/autocert"
)

// App is the interface provided by the web application.
type App interface {

	// Routes registers handlers for web request paths.
	Routes() http.Handler
}

// Server specifies the parameters for a web server.
type Server struct {

	// logging
	ErrorLog *log.Logger
	InfoLog  *log.Logger

	// HTTPS
	CertEmail string   // notifications from Let's Encrypt
	CertPath  string   // folder for certificates
	Domains   []string // domains to be served (empty for HTTP)

	// port addresses
	AddrHTTP  string
	AddrHTTPS string
}

// Serve runs the web server. It never returns.
func (srv *Server) Serve(app App) {

	// live server if we have a domain specified
	if len(srv.Domains) > 0 {

		// certificate manager
		m := &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(srv.Domains...),
			Cache:      autocert.DirCache(srv.CertPath),
			Email:      srv.CertEmail,
		}

		// web server
		srv.InfoLog.Printf("Starting server %s", srv.AddrHTTPS)

		// HTTPS server, with certificate from manager
		srv1 := newServer(srv.AddrHTTPS, app.Routes(), srv.ErrorLog, true)
		srv1.TLSConfig = &tls.Config{
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				// GoogleBot wants to connect without SNI. Use default name.
				if hello.ServerName == "" {
					hello.ServerName = srv.Domains[0]
				}
				return m.GetCertificate(hello)
			},

			// Preferences as recommended by Let's Go. No need to specify TLS1.3 suites.
			CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
			MinVersion:       tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
		}

		// HTTP server : accept http-01 challenges, and redirect HTTP -> HTTPS
		srv2 := newServer(srv.AddrHTTP, m.HTTPHandler(http.HandlerFunc(handleHTTPRedirect)), srv.ErrorLog, false)
		go srv2.ListenAndServe()

		// HTTPS server
		err := srv1.ListenAndServeTLS("", "")
		srv.ErrorLog.Fatal(err)

	} else {

		// web server
		srv.InfoLog.Printf("Starting server %s", srv.AddrHTTP)

		// just an HTTP server
		srv1 := newServer(srv.AddrHTTP, app.Routes(), srv.ErrorLog, true)

		err := srv1.ListenAndServe()
		srv.ErrorLog.Fatal(err)
	}

	// ## Add option with self-signed certificates
	// ## was: err = srv.ListenAndServeTLS("./tls/cert.pem", "./tls/key.pem")

}

// handleHTTPRedirect redirects HTTP requests to HTTPS.
// Copied from autocert and changed to do 301 redirect.
func handleHTTPRedirect(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "Use HTTPS", http.StatusBadRequest)
		return
	}
	target := "https://" + stripPort(r.Host) + r.URL.RequestURI()
	http.Redirect(w, r, target, http.StatusMovedPermanently)
}

func stripPort(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport
	}
	return net.JoinHostPort(host, "443")
}

// newServer makes an HTTP server, with appropriate timeout settings.
func newServer(addr string, handler http.Handler, log *log.Logger, main bool) *http.Server {

	// common server parameters for HTTP/HTTPS
	s := &http.Server{
		Addr:     addr,
		ErrorLog: log,
		Handler:  handler,
	}

	// set timeouts so that a slow or malicious client doesn't hold resources forever
	if main {

		// These are lax ones, but suggested in
		//   https://medium.com/@simonfrey/go-as-in-golang-standard-net-http-config-will-break-your-production-environment-1360871cb72b
		s.ReadHeaderTimeout = 20 * time.Second // this is the one that matters for SlowLoris?
		// ReadTimeout:  1 * time.Minute, // remove if variable timeouts in handlers
		s.WriteTimeout = 2 * time.Minute // starts after reading of request headers
		s.IdleTimeout = 2 * time.Minute

	} else {
		// tighter limits for HTTP certificate renewal and redirection to HTTPS
		s.ReadTimeout = 5 * time.Second   // remove if variable timeouts in handlers
		s.WriteTimeout = 10 * time.Second // starts after reading of request headers
		s.IdleTimeout = 1 * time.Minute
	}

	return s
}
