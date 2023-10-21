// Copyright © Rob Burke inchworks.com, 2020.

package users

// Form handling for user sign-up, login and management

import (
	"errors"
	"net/http"

	"github.com/inchworks/webparts/multiforms"
)

// GetFormLogin renders the form for a user to log in.
func (u *Users) GetFormLogin(w http.ResponseWriter, r *http.Request) {

	u.App.Render(w, r, "user-login.page.tmpl", multiforms.New(nil, u.App.Token(r)))
}

// PostFormLogin processes the log-in form.
func (u *Users) PostFormLogin(w http.ResponseWriter, r *http.Request) {

	app := u.App

	err := r.ParseForm()
	if err != nil {
		u.clientError(w, http.StatusBadRequest)
		return
	}

	// check username and password
	f := multiforms.New(r.PostForm, app.Token(r))
	username := f.Get("username")
	user, err := u.Store.GetNamed(username)
	if err == nil {
		err = user.authenticate(f.Get("password"))
	}

	// take care not to reveal whether it is the username or password that is wrong
	// We shouldn't record the name or password, in case it is a mistake by a legitimate user.
	if err != nil {
		if u.Store.IsNoRecord(err) || errors.Is(err, ErrInvalidCredentials) {
			app.LogThreat("login error", r)
			f.Errors.Add("generic", "Username or password not known")
			app.Render(w, r, "user-login.page.tmpl", f)

		} else {
			app.Log(err)
			u.clientError(w, http.StatusInternalServerError)
		}
		return
	}

	// add the user ID to the session, so that they are now 'logged in'
	app.Authenticated(r, user.Id)

	// get redirect path - probably the URL that the user accessed, or the home page (may show more, now logged in)
	http.Redirect(w, r, app.GetRedirect(r), http.StatusSeeOther)
}

// GetFormSignup renders the form for a pre-approved user to sign-up.
func (u *Users) GetFormSignup(w http.ResponseWriter, r *http.Request) {

	u.App.Render(w, r, "user-signup.page.tmpl", multiforms.New(nil, u.App.Token(r)))
}

// PostFormSignup processes the sign-up form.
func (u *Users) PostFormSignup(w http.ResponseWriter, r *http.Request) {

	app := u.App

	err := r.ParseForm()
	if err != nil {
		u.clientError(w, http.StatusBadRequest)
		return
	}

	// process form data
	f := multiforms.New(r.PostForm, u.App.Token(r))
	f.Required("displayName", "username", "password")
	f.MaxLength("displayName", 60)
	f.MaxLength("username", 60)
	//   form.MatchesPattern("email", forms.EmailRX)
	f.MinLength("password", 10)
	f.MaxLength("password", 60)

	// check if username known here
	// We don't record the username, in case it is a mistake by a legitimate user.
	username := f.Get("username")
	user, err := u.canSignup(username)
	if err != nil {

		app.LogThreat("signup error", r)
		f.Errors.Add("username", err.Error())
	}

	// If there are any errors, redisplay the signup form.
	if !f.Valid() {
		app.Render(w, r, "user-signup.page.tmpl", f)
		return
	}

	// add user
	err = u.onUserSignup(user, f.Get("displayName"), f.Get("password"))
	if err == nil {
		app.Flash(r, "Your sign-up was successful. Please log in.")

		http.Redirect(w, r, "/user/login", http.StatusSeeOther)
	} else {
		u.clientError(w, http.StatusBadRequest)
	}
}

// GetFormEdit renders the form to manage users.
func (u *Users) GetFormEdit(w http.ResponseWriter, r *http.Request) {

	app := u.App

	// form to edit users, and
	f := u.forEditUsers(app.Token(r))

	// display form
	app.Render(w, r, "edit-users.page.tmpl", f)
}

// PostFormUsers processes the form with changes to users.
func (u *Users) PostFormEdit(w http.ResponseWriter, r *http.Request) {

	app := u.App

	err := r.ParseForm()
	if err != nil {
		u.clientError(w, http.StatusBadRequest)
		return
	}

	// process form data
	f := u.NewUsersForm(r.PostForm, u.App.Token(r))
	users, err := f.GetUsers(len(u.Roles))
	if err != nil {
		app.Log(err)
		u.clientError(w, http.StatusBadRequest)
		return
	}

	// redisplay form if data invalid
	if !f.Valid() {
		app.Render(w, r, "edit-users.page.tmpl", f)
		return
	}

	// save changes
	if tx := u.onEditUsers(users); tx != 0 {
		if err := u.TM.Do(tx); err != nil {
			u.clientError(w, http.StatusInternalServerError)
			return
		}
		app.Flash(r, "User changes saved.")
		http.Redirect(w, r, "/", http.StatusSeeOther)

	} else {
		u.clientError(w, http.StatusBadRequest)
	}
}

// clientError request rollback of any updates, and sends a status code and description to the user.
func (u *Users) clientError(w http.ResponseWriter, status int) {

	u.Store.Rollback()
	http.Error(w, http.StatusText(status), status)
}
