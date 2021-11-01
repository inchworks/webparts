// Copyright © Rob Burke inchworks.com, 2020.

package multiforms

// Validation error messages for forms, keyed by name of the form field.

type formErrors map[string][]string
type childErrors map[string]map[int][]string

// Add appends an error message for a given field
func (e formErrors) Add(field, message string) {
	e[field] = append(e[field], message)
}

// Add appends an error message for a given child field.
func (e childErrors) Add(field string, ix int, message string) {
	// allow the child maps to be nil
	if e[field] == nil {
		e[field] = make(map[int][]string)
	}

	e[field][ix] = append(e[field][ix], message)
}

// Get retrieves the first error message for a specified field from the map.
func (e formErrors) Get(field string) string {
	es := e[field]
	if len(es) == 0 {
		return ""
	}
	return es[0]
}

// Get retrieves the first error message for a specified child field from the map.
func (e childErrors) Get(field string, ix int) string {
	es := e[field][ix]
	if len(es) == 0 {
		return ""
	}
	return es[0]
}

// Valid returns a CSS class to indicate if field is invalid, for Bootstrap.
// It is needed because Bootstrap won't display an invalidity message unless field is marked invalid.
func (e formErrors) Valid(field string) string {

	// ## This is pain to handle. Cannot hard-code the message in the template because the error may vary.

	es := e[field]
	if len(es) == 0 {
		return ""
	}
	return "is-invalid"
}
