// Copyright © Rob Burke inchworks.com, 2020.

package users

import (
	"net/url"

	"github.com/inchworks/webparts/multiforms"
)

type UsersForm struct {
	multiforms.Form
	RoleOpts   []string
	StatusOpts []string
	Children   []*UserFormData
	App        interface{}
}

type UserFormData struct {
	multiforms.Child
	Username    string
	DisplayName string
	NUser       int64
	Role        int
	Status      int
}

var statusOpts = []string{"suspended", "known", "active"}

// NewUsersForm returns a form to edit users.
func (u *Users) NewUsersForm(data url.Values, token string) *UsersForm {
	return &UsersForm{
		Form:       *multiforms.New(data, token),
		RoleOpts:   u.Roles,
		StatusOpts: statusOpts,
		Children:   make([]*UserFormData, 0, 16),
	}
}

// Add appends a user sub-form to the form.
func (f *UsersForm) Add(index int, u *User) {

	f.Children = append(f.Children, &UserFormData{
		Child:       multiforms.Child{Parent: &f.Form, ChildIndex: index},
		Username:    u.Username,
		DisplayName: u.Name,
		NUser:       u.Id,
		Role:        u.Role,
		Status:      u.Status,
	})
}

// AddTemplate appends the sub-form template for new users.
func (f *UsersForm) AddTemplate() {

	f.Children = append(f.Children, &UserFormData{
		Child:  multiforms.Child{Parent: &f.Form, ChildIndex: -1},
		Status: UserKnown,
	})
}

// GetUsers returns the user data as an array of structs.
// They are sent in the HTML form as arrays of values for each field name.
func (f *UsersForm) GetUsers(nRoleOpts int) (items []*UserFormData, err error) {

	nItems := f.NChildItems()

	for i := 0; i < nItems; i++ {

		ix, err := f.ChildIndex("index", i)
		if err != nil {
			return nil, err
		}

		role, err := f.ChildSelect("role", i, ix, nRoleOpts)
		if err != nil {
			return nil, err
		}

		status, err := f.ChildSelect("status", i, ix, len(statusOpts))
		if err != nil {
			return nil, err
		}

		items = append(items, &UserFormData{
			Child:       multiforms.Child{Parent: &f.Form, ChildIndex: ix},
			Username:    f.ChildRequired("username", i, ix),
			DisplayName: f.ChildRequired("displayName", i, ix),
			Role:        role,
			Status:      status,
		})
	}

	// Add the child items back into the form, in case we need to redisplay it
	f.Children = items

	return items, nil
}
