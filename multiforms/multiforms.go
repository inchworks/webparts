// Copyright © Rob Burke inchworks.com, 2020.

// Package multiforms processing returned HTML forms that may contain child forms.
//
// Acknowledgement: the origin of this code is the book Let's Go! by Alex Edwards.
// This version adds support for child forms.

package multiforms

// Well, this much more complex than I like.
// o Variable items in the template are set directly.
// o Field values are set from a map in Form, because that is how they are returned on Post,
//   and we need to send the same values back to the client when there is an error.
// o But child form values are awkward to work with as arrays of values per field name, so
//   we always unpack them into structs when the form is received. We use the same structs
//   to contruct the template.
// o Errors (for parent and child) are mostly null, so held in maps within Form.
//   ## Keeps the child errors away from the Child struct, but they could have gone there instead,
//   and then Add and Get for them would have looked more like access to parent errors.
// o ## Tidier to put Child and its methods in another file?
// o Must rember to create a template item (index -1( when building the child structs,
//   and to skip it when processing the returned form.
// o ## Should some of the child processing be pushed down into formSlides.go?

import (
	"embed"
	"errors"
	"fmt"
	"html/template"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// EmailRX is a regular expression for an email address, as recommended by W3C and Web Hypertext Application Technology Working Group.
var EmailRX = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// Form holds form data (in url.Values) and validation errors.
type Form struct {
	url.Values
	CSRFToken   string
	Errors      formErrors
	ChildErrors childErrors
}

// Child specifies a child form.
type Child struct {
	Parent     *Form
	ChildIndex int
}

// WebFiles are the package's web resources (templates and static files)
//go:embed web
var WebFiles embed.FS

// New returns an initialised Form structure.
func New(data url.Values, token string) *Form {
	return &Form{
		Values:      data,
		CSRFToken:   token,
		Errors:      make(map[string][]string),
		ChildErrors: make(map[string]map[int][]string),
	}
}

// ChildGet is deprecated. Use ChildText.
func (f *Form) ChildGet(field string, i int) string {

	return f.Values[field][i] // url.Values is a map[string][]string. First item is the template.
}

// ChildIndex returns the index of a child form.
// Indexes are used to match the returned child items against the original data.
func (f *Form) ChildIndex(field string, i int) (int, error) {

	ix, err := strconv.Atoi(f.Values[field][i])

	if err != nil {
		return 0, err

	} else if ix < -1 {
		// not template or positive
		return 0, errors.New("Bad child index in form")
	}

	return ix, nil
}

// NChildItems returns the number of child items in the form.
func (f *Form) NChildItems() int {

	return len(f.Values["index"])
}

// ChildBool returns a checkbox value from child form.
// Unlike other fields, only checked fields are returned, and the value is the child index.
func (f *Form) ChildBool(field string, ix int) bool {

	// ignore template
	if ix == -1 {
		return false
	}

	// ## Better to convert the returned checkbox values to ints just once.
	ixStr := strconv.Itoa((ix))

	// a value returned means checked
	for _, v := range f.Values[field] {
		if v == ixStr {
			return true
		}
	}
	return false
}

// ChildFile returns a file name from child form.
func (f *Form) ChildFile(field string, i int, ix int, validType func(string) bool) string {

	// ## Could be a general-purpose validation, given an error string.

	// don't validate template
	if ix == -1 {
		return ""
	}

	value := f.Values[field][i]

	if value != "" && !validType(value) {
		f.ChildErrors.Add(field, ix, "File type not supported: ")
	}
	return value
}

// ChildMin returns a number with a minimum value from a child form.
func (f *Form) ChildMin(field string, i int, ix int, min int) int {

	// don't validate template
	if ix == -1 {
		return 0
	}

	n, err := strconv.Atoi(f.Values[field][i])

	if err != nil {
		f.ChildErrors.Add(field, ix, "Must be a number")

	} else if n < min {
		f.ChildErrors.Add(field, ix, fmt.Sprintf("%d or more", min))
	}

	return n
}

// ChildPositive returns a positive number from a child form.
func (f *Form) ChildPositive(field string, i int, ix int) int {

	// don't validate template
	if ix == -1 {
		return 0
	}

	n, err := strconv.Atoi(f.Values[field][i])

	if err != nil {
		f.ChildErrors.Add(field, ix, "Must be a number")

	} else if n < 0 {
		f.ChildErrors.Add(field, ix, "Cannot be negative")
	}

	return n
}

// ChildRequired is deprecated. Use ChildText.
func (f *Form) ChildRequired(field string, i int, ix int) string {

	// don't validate template
	if ix == -1 {
		return ""
	}

	value := strings.TrimSpace(f.Values[field][i])
	if value == "" {
		f.ChildErrors.Add(field, ix, "Cannot be blank")
	}
	return value
}

// ChildSelect returns of the value of an HTML select.
// It assumes values are integers, 0 ... nOption-1
func (f *Form) ChildSelect(field string, i int, ix int, nOptions int) (int, error) {

	// don't validate template
	if ix == -1 {
		return 0, nil
	}

	s := f.Values[field][i]

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if n < 0 || n >= nOptions {
		return 0, errors.New("Multiforms: Unexpected option in select")
	}

	return n, nil
}

// ChildText returns trimmed text a child form, validating the value length.
// Set min=1 for a required value, max<=0 for no upper size.
func (f *Form) ChildText(field string, i int, ix int, min int, max int) string {

	// don't validate template
	if ix == -1 {
		return ""
	}

	// url.Values is a map[string][]string. First item is the template.
	value := strings.TrimSpace(f.Values[field][i])

	// validate length
	l := utf8.RuneCountInString(value)
	if min > 0 {
		if l == 0 {
			f.ChildErrors.Add(field, ix, "Cannot be blank")
		} else if l < min {
			f.ChildErrors.Add(field, ix, fmt.Sprintf("Too short (minimum %d characters)", min))
		}
	}
	if max > 0 && l > max {
		f.ChildErrors.Add(field, ix, fmt.Sprintf("Too long (maximum %d characters)", max))
	}

	return value
}

// ChildTrimmed is deprecated. Use ChildText.
func (f *Form) ChildTrimmed(field string, i int) string {

	return strings.TrimSpace(f.Values[field][i])
}

// Check that field value is float within range
func (f *Form) Float(s string, field string, min float64, max float64) float64 {
	n, err := strconv.ParseFloat(s, 64)

	if err != nil {
		f.Errors.Add(field, "Must be a number")

	} else if n < min {
		f.Errors.Add(field, "Too small")

	} else if n >= max {
		f.Errors.Add(field, "Too large")
	}

	return n
}

// MatchesPattern checks that a field matches a regular expression.
func (f *Form) MatchesPattern(field string, pattern *regexp.Regexp) {
	value := f.Get(field)
	if value == "" {
		return
	}
	if !pattern.MatchString(value) {
		f.Errors.Add(field, "This field is invalid")
	}
}

// MaxLength checks that a field does not exceed a maximum number of characters.
func (f *Form) MaxLength(field string, d int) {
	value := f.Get(field)
	if value == "" {
		return
	}
	if utf8.RuneCountInString(value) > d {
		f.Errors.Add(field, fmt.Sprintf("Too long (maximum %d characters)", d))
	}
}

// MinLength checks that a field contains a minimum number of characters.
func (f *Form) MinLength(field string, d int) {
	value := f.Get(field)
	if value == "" {
		return
	}
	if utf8.RuneCountInString(value) < d {
		f.Errors.Add(field, fmt.Sprintf("Too short (minimum is %d characters)", d))
	}
}

// Positive checks that a field value is integer and >=0
func (f *Form) Positive(field string) int {
	s := f.Get(field)
	i, err := strconv.Atoi(s)

	if err != nil {
		f.Errors.Add(field, "Must be a number")

	} else if i < 0 {
		f.Errors.Add(field, "Cannot be negative")
	}

	return i
}

// Required checks that specific fields in the form data are present and not blank.
func (f *Form) Required(fields ...string) {
	for _, field := range fields {
		value := f.Get(field)
		if strings.TrimSpace(value) == "" {
			f.Errors.Add(field, "Cannot be blank")
		}
	}
}

// PermittedValues checks that a specific field in the form
// matches one of a set of specific permitted values.
func (f *Form) PermittedValues(field string, opts ...string) {
	value := f.Get(field)
	if value == "" {
		return
	}
	for _, opt := range opts {
		if value == opt {
			return
		}
	}
	f.Errors.Add(field, "Value not permitted")
}

// Valid returns true the form data has no errors.
func (f *Form) Valid() bool {
	return len(f.Errors)+len(f.ChildErrors) == 0
}

// ChildError returns the error for a child field
func (c *Child) ChildError(field string) string {

	// save curren index for error report

	return c.Parent.ChildErrors.Get(field, c.ChildIndex)
}

// ChildStyle gets the display attribute for child item - hidden for a child form template.
func (c *Child) ChildStyle() template.HTMLAttr {

	var s string
	if c.ChildIndex == -1 {
		s = "style='display:none'"
	}

	return template.HTMLAttr(s)
}

// ChildValid returns a CSS class to indicate if a field is valid, for Bootstrap.
func (c *Child) ChildValid(field string) string {

	// ## Horrible!

	// save curren index for error report
	es := c.Parent.ChildErrors.Get(field, c.ChildIndex)
	if len(es) == 0 {
		return ""
	}
	return "is-invalid"
}
