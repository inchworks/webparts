// Copyright © Rob Burke inchworks.com, 2021.

package stack

import (
	"io/fs"
)

// StackFS provides a single file system from a stack of file systems.
// A file defined higher in the stack overrides any file of the same path and name lower in the stack.
// Typically the top FS holds site customisation, the next lower FS holds embedded files
// for application resources, with package resources at the bottom.
//
// After initialisation, each Open request accesses just one file system.
// The implementation assumes that all the FS, except for the top one, hold a modest number of files.
// It also assumes that only the top FS may have files added after initialisation,
// and removing a file will not uncover one in a lower FS.
type StackFS struct {
	stacked map[string]fs.FS
	top     fs.FS
}

// New returns a combined file system for a stack of file systems (base first).
func NewFS(stack ...fs.FS) (fs.FS, error) {

	sfs := StackFS{ stacked: make(map[string]fs.FS, 16) }

	for n, fsys := range stack {

		// the last file system is the top one, holding any files added after initialisation
		top := (n == len(stack)-1)
		if top {
			sfs.top = fsys
		}

		// scan files for a stacked file system
		err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {

			if err != nil {
				return err
			}

			if !d.IsDir() {
				if !top {
					// add file to stack, overwriting any lower entry for this file
					sfs.stacked[path] = fsys
				} else {
					// remove from lower in stack, if present
					delete(sfs.stacked, path)
				}
			}

			return nil // file OK
		})

		if err != nil && !top {
			return nil, err // it is OK if the site directory doesn't exists
		}
	}
	return sfs, nil
}

// Open opens the named file.
func (sfs StackFS) Open(name string) (fs.File, error) {

	// check the stack for a known file
	fsys := sfs.stacked[name]
	if fsys != nil {
		return fsys.Open(name)
	} else {
		// unknown files are in the top FS
		return sfs.top.Open(name)
	}
}
