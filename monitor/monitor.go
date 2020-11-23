// Copyright © Rob Burke inchworks.com, 2020.

// Package monitor maintains and reports the liveness of a set of clients that are polling a server.
package monitor

import (
	"sync"
	"time"
)

const (
	monitorPeriod  = 60 // monitor reporting period (seconds)
	monitorPeriods = 5  // no of reporing periods

	amberMissed = 0.05 // max proportion missed, for amber status
	redMissed   = 20   // max number missed consecutively, for red status
)

// Period reports the status of a client for a monitoring period,
type Period struct {
	Lost    int64 // excluding current outage
	Missed  int64 // missed in current outage
	Longest int64 // longest outage
	Status  string
	start   time.Time // period start
}

// Monitor reports the status of a client for a set of monitoring periods.
type Monitored struct {
	Name         string
	Periods      [monitorPeriods]Period
	halfInterval time.Duration
	last         time.Time
}

type Monitor struct {
	mu      sync.Mutex
	names   map[string]int
	clients []Monitored
}

// Init starts the monitor. It returns function to be called to stop the monitor.
func (m *Monitor) Init() func() {

	m.names = make(map[string]int)

	// monitoring periods
	ticker := time.NewTicker(monitorPeriod * time.Second)
	quit := make(chan struct{})
	go func() {

		for {
			select {
			case <-ticker.C:
				m.endPeriod()

			case <-quit:
				// ## drain ticks?
				return
			}
		}
	}()

	// cleanup at end
	return func() {

		// stop the ticker and terminate worker
		close(quit)
		ticker.Stop()
	}
}

// Alive is called on each client request, to show that it is alive.
func (m *Monitor) Alive(clientIx int) {

	m.mu.Lock()
	defer m.mu.Unlock()

	// validate client index (could be an old display still running)
	if clientIx < 0 || clientIx >= len(m.clients) {
		return
	}

	m.aliveLocked(clientIx)
}

// Register adds a client to monitoring. It may be called for an existing client.
func (m *Monitor) Register(name string, tickInterval time.Duration) int {

	m.mu.Lock()
	defer m.mu.Unlock()

	ix, ok := m.names[name]
	if ok {
		// already registered, treat as alive
		m.aliveLocked(ix)
	} else {

		// new client
		c := Monitored{
			Name:         name,
			halfInterval: tickInterval / 2,
			last:         time.Now(),
		}
		c.Periods[0] = Period{start: time.Now()}

		// add to array of clients
		m.clients = append(m.clients, c)
		ix = len(m.clients) - 1

		// for name lookup
		m.names[name] = ix
	}
	return ix
}

// Status returns client statuses, for reporting.
func (m *Monitor) Status() []Monitored {

	m.mu.Lock()
	defer m.mu.Unlock()

	// update statuses
	m.updateStatuses()

	return m.clients // a copy of the client statuses (I trust)
}

// Client is alive (called with lock).
func (m *Monitor) aliveLocked(clientIx int) {

	now := time.Now()

	c := &m.clients[clientIx]
	c.update(true)

	c.last = now
}

// Processing at end of each period.
func (m *Monitor) endPeriod() {

	m.mu.Lock()
	defer m.mu.Unlock()

	// update statuses
	m.updateStatuses()

	now := time.Now()

	for i := range m.clients {
		c := &m.clients[i]

		// save current period ..
		for j := monitorPeriods - 1; j >= 1; j-- {
			c.Periods[j] = c.Periods[j-1] // copy back
		}

		// .. and start a new one
		c.Periods[0] = Period{start: now}
	}
}

// Set current status for each client.
func (m *Monitor) updateStatuses() {

	// evaluate status for each client
	for i := range m.clients {
		c := &m.clients[i]

		// check max missed (red) and % missed (amber)
		p := c.update(false)
		if p.Longest >= redMissed {
			p.Status = "R"

		} else if since := c.halfIntervalsSince(p.start) / 2; since > 0 &&
			float32(p.Lost+p.Missed)/float32(since) > amberMissed {
			p.Status = "A"

		} else {
			p.Status = "G"
		}
	}
}

// Half-intervals since time t.
func (c *Monitored) halfIntervalsSince(t time.Time) int64 {

	return time.Since(t).Nanoseconds() / c.halfInterval.Nanoseconds()
}

// Update monitoring statistics.
func (c *Monitored) update(alive bool) *Period {

	p := &c.Periods[0]

	// count missing alive calls from start of period
	var last time.Time
	if c.last.Sub(p.start) > 0 {
		last = c.last
	} else {
		last = p.start
	}

	// check if ticks are late (ok to be up to one half-interval late)
	e := c.halfIntervalsSince(last) // elapsed in half intervals
	if e > 2 {
		// no of intervals missed
		missed := e / 2

		if alive {
			// capture missed from last time, if last is being updated
			p.Lost += missed
			p.Missed = 0
		} else {
			p.Missed = missed
		}

		// worst case intervals missed
		if missed > p.Longest {
			p.Longest = missed
		}
	}

	return p
}
