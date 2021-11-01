// Copyright © Rob Burke inchworks.com, 2021.

package stack

import (
	"html/template"
	"io/fs"
	"path/filepath"
)

// NewTemplates returns a cache of HTML page templates for an application, with added package and site templates.
//
// The cache is built using the file organisation suggested by Let's Go by Alex Edwards:
// a page.tmpl file specifies the name and content of am HTML page in the cache;
// a layout.tmpl file defines a common layout for a set of pages;
// partial.tmpl files define common content across multiple pages and layouts.
//
// Application template definitions override package templates of the same name.
// Similarly, site template definitions override application templates by name.
func NewTemplates(forPkgs []fs.FS, forApp fs.FS, forSite fs.FS, funcs template.FuncMap) (map[string]*template.Template, error) {

	// cache of templates indexed by page name
	cache := map[string]*template.Template{}

	// add library page templates
	for _, forPkg := range forPkgs {
		if err := addTemplates(cache, forPkg, funcs, forPkg, forApp, forSite); err != nil {
			return nil, err
		}
	}

	// add application page templates
	if err := addTemplates(cache, forApp, funcs, forApp, forSite); err != nil {
		return nil, err
	}

	// add site-specific page templates
	if err := addTemplates(cache, forSite, funcs, forApp, forSite); err != nil {
		return nil, err
	}

	// return the map
	return cache, nil
}

// addTemplates parses a set of template files for HTML pages.
// It adds template definitions from a number of layers (typically a package, the app, and site customisation).
func addTemplates(cache map[string]*template.Template, pages fs.FS, funcs template.FuncMap, layers ...fs.FS) error {

	// get the set of 'page' templates
	pgs, err := fs.Glob(pages, "*.page.tmpl")
	if err != nil {
		return err
	}

	for _, pg := range pgs {

		// extract the file name (e.g. 'home.page.tmpl') from the full file path
		name := filepath.Base(pg)

		// The template.FuncMap must be registered with the template set before calling ParseFiles().
		// So we create an empty template set, use the Funcs() method to register the map, and then parse the file.

		// parse the page template file in to a template set
		ts, err := template.New(name).Funcs(funcs).ParseFS(pages, pg)
		if err != nil {
			return err
		}

		// add 'layout' template files to the template set
		// (Typically only one of these will be needed, but we leave the template implementation to link it.)
		for _, l := range layers {
			if ts, err = parseIf(ts, l, "*.layout.tmpl"); err != nil {
				return err
			}
		}

		// add 'partial' template files to the template set
		for _, l := range layers {
			if ts, err = parseIf(ts, l, "*.partial.tmpl"); err != nil {
				return err
			}
		}

		// add the template set for the page to the cache, keyed by the file name
		cache[name] = ts
	}

	return nil
}

// parseIf checks if any files match the pattern, and then calls template.ParseFS.
// Inconveniently, ParseFS requires at least one template file :-(.
func parseIf(ts *template.Template, set fs.FS, pattern string) (*template.Template, error) {

	// (ParseFS doesn't accept an empty set)
	ms, _ := fs.Glob(set, pattern)
	if len(ms) > 0 {
		return ts.ParseFS(set, pattern)
	} else {
		return ts, nil
	}
}
