package disk

import (
	"fmt"
	"os"
)

// GetDirContents returns a list of strings of each entry in the given path, or
// any error from os.ReadDir().
//
// If fullpath is true, the full path is appended, if false, only the item name
// is appended.
func GetDirContents(path string, fullPath bool) ([]string, error) {
	var contents []string

	items, err := os.ReadDir(path)
	if err != nil {
		return []string{}, err
	}

	for _, item := range items {
		var entry string
		if fullPath {
			entry = fmt.Sprintf("%s/%s", path, item.Name())
		} else {
			entry = item.Name()
		}
		contents = append(contents, entry)
	}

	return contents, nil
}
