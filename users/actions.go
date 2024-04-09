// Copyright © Rob Burke inchworks.com, 2020.

package users

// Processing for user data in a web server application.

import (
	"errors"
	"net/url"
	"time"

	"github.com/inchworks/webparts/v2/etx"
)

// UserDisplayName returns the display name for a user.
func (u *Users) UserDisplayName(userId int64) string {

	// serialisation
	defer u.App.Serialise(false)()

	r, _ := u.Store.Get(userId)

	return r.Name
}

// forEditUsers returns data to edit users in a form.
func (u *Users) forEditUsers(token string) *UsersForm {

	// serialisation
	defer u.App.Serialise(false)()

	// users
	users := u.Store.ByName()

	// form
	var d = make(url.Values)
	f := u.NewUsersForm(d, token)

	// add template and users to form
	f.AddTemplate()
	for i, u := range users {
		f.Add(i, u)
	}

	return f
}

// onEditUsers processes returned form data. Returns an extended transaction ID if there are no errors (client or server).
// ## Why not take the whole form?
func (ua *Users) onEditUsers(usSrc []*UserFormData) etx.TxId {

	app := ua.App

	// start extended transaction, for app to use as needed
	tx := ua.TM.Begin()

	// serialisation
	defer app.Serialise(true)()

	// skip template
	iSrc := 1
	iDest := 0

	// compare modified users against current users, and update
	usDest := ua.Store.ByName()
	nSrc := len(usSrc)
	nDest := len(usDest)

	for iSrc < nSrc || iDest < nDest {

		if iSrc == nSrc {
			// no more source users - delete from destination
			ua.App.OnRemoveUser(tx, usDest[iDest])
			ua.Store.DeleteId(usDest[iDest].Id)
			iDest++

		} else if iDest == nDest {
			// no more destination users - add new user
			u := User{
				Name:     usSrc[iSrc].DisplayName,
				Username: usSrc[iSrc].Username,
				Role:     usSrc[iSrc].Role,
				Status:   usSrc[iSrc].Status,
				Password: []byte(""),
			}
			ua.Store.Update(&u)
			ua.App.OnAddUser(tx, &u)
			iSrc++

		} else {
			ix := usSrc[iSrc].ChildIndex
			if ix > iDest {
				// source user removed - delete from destination
				ua.App.OnRemoveUser(tx, usDest[iDest])
				ua.Store.DeleteId(usDest[iDest].Id)
				iDest++

			} else if ix == iDest {
				// check if user's details changed
				uSrc := usSrc[iSrc]
				uDest := usDest[iDest]
				if uSrc.DisplayName != uDest.Name ||
					uSrc.Username != uDest.Username ||
					uSrc.Role != uDest.Role ||
					uSrc.Status != uDest.Status {

					was := *uDest
					uDest.Name = uSrc.DisplayName
					uDest.Username = uSrc.Username
					uDest.Role = uSrc.Role
					uDest.Status = uSrc.Status
					if err := ua.Store.Update(uDest); err != nil {
						return 0 // unexpected database error
					}
					ua.App.OnUpdateUser(tx, &was, uDest)
				}
				iSrc++
				iDest++

			} else {
				// out of sequence team index
				app.Rollback()
				return 0
			}
		}
	}

	return tx
}

// onUserSignup processes a sigup request.
func (u *Users) onUserSignup(username string, displayName string, password string) error {

	// serialisation
	defer u.App.Serialise(true)()

	// check if username known here
	// We don't record the username, in case it is a mistake by a legitimate user.
	user, err := u.Store.GetNamed(username)
	if err != nil {
		return errors.New("Not recognised. Ask us for an invitation.")
	}

	switch user.Status {
	case UserKnown:
		// OK

	case UserActive:
		return errors.New("Already signed up. You can log in.")

	case UserSuspended:
		return errors.New("Access suspended. Contact us.")

	default:
		panic("Unknown user status")
	}

	// set details for active user
	user.Name = displayName
	user.SetPassword(password) // encrypted password
	user.Status = UserActive
	user.Created = time.Now()

	return u.Store.Update(user)
}
