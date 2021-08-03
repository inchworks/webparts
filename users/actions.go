// Copyright © Rob Burke inchworks.com, 2020.

package users

// Processing for user data in a web server application.
//
// #### These functions may modify application state.
// #### Which functions need to be exported?

import (
	"errors"
	"net/url"
	"time"
)

// UserDisplayName returns the display name for a user.
func (u *Users) UserDisplayName(userId int64) string {

	// serialisation
	defer u.App.Serialise(false)()

	r, _ := u.Store.Get(userId)

	return r.Name
}

// canSignup checks if the specified username can sign up. Returns the user's record.
func (u *Users) canSignup(username string) (*User, error) {

	// serialisation
	defer u.App.Serialise(false)()

	user, err := u.Store.GetNamed(username)
	if err != nil {
		return nil, errors.New("Not recognised. Ask us for an invitation.")
	}

	switch user.Status {
	case UserKnown:
		// OK

	case UserActive:
		return nil, errors.New("Already signed up. You can log in.")

	case UserSuspended:
		return nil, errors.New("Access suspended. Contact us.")

	default:
		panic("Unknown user status")
	}

	return user, nil
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

// onEditUsers processes returned form data. Returns true if there are errors (client or server).
// ## Why not take the whole form?
func (ua *Users) onEditUsers(usSrc []*UserFormData) bool {

	app := ua.App

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
			ua.App.OnRemoveUser(usDest[iDest])
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
			iSrc++

		} else {
			ix := usSrc[iSrc].ChildIndex
			if ix > iDest {
				// source user removed - delete from destination
				ua.App.OnRemoveUser(usDest[iDest])
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

					uDest.Name = uSrc.DisplayName
					uDest.Username = uSrc.Username
					uDest.Role = uSrc.Role
					uDest.Status = uSrc.Status
					if err := ua.Store.Update(uDest); err != nil {
						return false // unexpected database error
					}
				}
				iSrc++
				iDest++

			} else {
				// out of sequence team index
				return false
			}
		}
	}

	return true
}

// onUserSignup processes a sigup request.
//
// #### Assumes serialisation started earlier
func (u *Users) onUserSignup(user *User, name string, password string) error {

	// serialisation
	// #### should serialise from call to CanSignup
	defer u.App.Serialise(true)()

	// set details for active user
	user.Name = name
	user.SetPassword(password) // encrypted password
	user.Status = UserActive
	user.Created = time.Now()

	return u.Store.Update(user)
}
