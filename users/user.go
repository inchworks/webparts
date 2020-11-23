package users

// Functions on a single user's data

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

var ErrInvalidCredentials = errors.New("webparts/users: invalid credentials")

// authenticate checks a password against the stored hash
func (us *User) authenticate(pwd string) error {

	// must be an active user
	if us.Status < UserActive {
		return ErrInvalidCredentials
	}

	// check password
	err := bcrypt.CompareHashAndPassword(us.Password, []byte(pwd))
	if err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return ErrInvalidCredentials
		} else {
			return err
		}
	}
	return nil
}

// setPassword stores a password hash
func (us *User) SetPassword(pwd string) error {

	hashed, err := bcrypt.GenerateFromPassword([]byte(pwd), 12)
	if err != nil {
		return err
	} else {
		us.Password = hashed
	}
	return nil
}
