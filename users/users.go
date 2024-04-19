// Copyright © Rob Burke inchworks.com, 2020.

// Package users implements user management and verification for a web application.
package users

import (
	"embed"
	"net/http"
	"time"

	"github.com/inchworks/webparts/v2/etx"
)

// App is the interface to functions provided by the parent application.
type App interface {
	// Authenticated adds a logged-in user's ID to the session
	Authenticated(r *http.Request, id int64)

	// Flash adds a confirmation message to the next page, via the session
	Flash(r *http.Request, msg string)

	// GetRedirect returns the next page after log-in, probably from a session key
	GetRedirect(r *http.Request) string

	// Log optionally records an error
	Log(error)

	// LogThreat optionally records a rejected request to sign-up or log-in
	LogThreat(msg string, r *http.Request)

	// OnAddUser is called to add any additional application data for a user.
	// It is called after the store request has been made.
	OnAddUser(tx etx.TxId, user *User)

	// OnRemoveUser is called to delete any application data for a user.
	// It is called before the store entry is deleted.
	OnRemoveUser(tx etx.TxId, user *User)

	// OnUpdateUser is called to do any application processing when a user is updated.
	// It is called after the store request has been made.
	OnUpdateUser(tx etx.TxId, from *User, to *User)

	// Render writes an HTTP response using the specified template and template field Users
	Render(w http.ResponseWriter, r *http.Request, template string, usersData interface{})

	// Rollback requests that the transaction started by Serialise be cancelled.
	Rollback()

	// Serialise optionally requests application-level serialisation.
	// If updates=true, the store is to be updated and a transaction might be started (especially if a user is to be added or deleted).
	// The returned function will be called at the end of the operation.
	Serialise(updates bool) func()

	// Token returns a token to be added to the form as the hidden field csrf_token.
	Token(r *http.Request) string
}

const (
	// user status values
	UserRemoved   = -10 // deletion in progress but cached access allowed
	UserSuspended = 0   // blocked from access or registration
	UserKnown     = 1   // allowed to register and set display name and password
	UserActive    = 2   // registered

	MaxName = 60 // maximum name characters
)

// User struct holds the stored data for a user.
type User struct {
	Id       int64     // database ID
	Parent   int64     // parent ID, if multiple sets of user are supported
	Username string    // unique name for user, typically an email address
	Name     string    // display name for user
	Role     int       // user's role (normal, administrator, etc.)
	Status   int       // user's status
	Password []byte    // hashed password
	Created  time.Time // time of first registration
}

// UserStore is the interface for storage and update of user information.
// To be implemented by the parent application.
// Id and Username are unique keys for a user.
type UserStore interface {
	ByName() []*User                         // all users, in name order
	DeleteId(id int64) error                 // delete user
	Get(id int64) (*User, error)             // get user by database ID
	GetNamed(username string) (*User, error) // get user for username (expected to be unique)
	IsNoRecord(error) bool                   // true if error is "record not found"
	Name(id int64) string                    // get display name for user by database ID
	Rollback()                               // (redundant)
	Update(s *User) error                    // add or update user
}

// Users holds the dependencies of this package on the parent application.
// It has no state of its own.
type Users struct {
	App   App
	Roles []string
	Store UserStore
	TM    *etx.TM
}

// WebFiles are the package's web resources (templates and static files)
//
//go:embed web
var WebFiles embed.FS
