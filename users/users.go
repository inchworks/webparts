// Copyright © Rob Burke inchworks.com, 2020.

// Package users implements user management and verification for a web application.
package users

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"
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

	// OnAddUser is called to add any additional application data for a iser

	// OnRemoveUser is called to delete any application data for a user
	OnRemoveUser(user *User)

	// Render writes an HTTP response using the specified template and template field Users
	Render(w http.ResponseWriter, r *http.Request, template string, usersData interface{})

	// Serialise optionally requests application-level serialisation.
	// If updates=true, the store is to be updated and a transaction might be started (especially if a user is to be added or deleted).
	// The returned function will be called at the end of the operation.
	Serialise(updates bool) func()

	// Token returns a token to be added to the form as the hidden field csrf_token.
	Token(r *http.Request) string
}

const (
	// user status values
	UserSuspended = 0 // blocked from access or registration
	UserKnown     = 1 // allowed to register and set display name and password
	UserActive    = 2 // registered
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
	ByName() []*User                                // all users, in name order
	DeleteId(id int64) error						// delete user
	Get(id int64) (*User, error)                    // get user by database ID
	GetNamed(username string) (*User, error) // get user for username (expected to be unique)
	IsNoRecord(error) bool							// true if error is "record not found"
	Name(id int64) string                           // get display name for user by database ID
	Rollback()                                      // request update rollback
	Update(s *User) error                           // add or update user
	// ## Contributors !!
}

// Users holds the dependencies of this package on the parent application.
// It has no state of its own.
type Users struct {
	App   App
	Roles []string
	Store UserStore
}

// TemplatesPath returns a path to the package's template files, if accessible.
//
// They will not be available if running without source code. In this case the parent
// must use a copy made during the application build.
func TemplatesPath() (string, error) {

	// get the file for this function
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", nil // don't know why, but not worth worrying
	}

	// templates folder, relative to this file
	tp := filepath.Join(filepath.Dir(filename), "web/template")

	// check if folder exists
	_, err := os.Stat(tp)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err // don't know!
		} else {
			return "", nil // no folder
		}
	}
	return tp, nil // folder exists
}
