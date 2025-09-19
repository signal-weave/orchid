package paths

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// -----------------------------------------------------------------------------
// Shared, paths/directories that are referenced between packages.
// -----------------------------------------------------------------------------

// -------Database Paths--------------------------------------------------------

// The directory the program is running from.
func GetExecDirectory() string {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	exeDir := filepath.Dir(exePath)
	return exeDir
}

// The directory the .exe file is ran from.
// Used as default DatabasePath if one is not provided.
var ExeDir = GetExecDirectory()

// Where the currently loaded database files are located. This can be set
// through CLI but will default to the .exe's directory.
var DatabasePath string = ExeDir

// GetTablePath returns the path to the .db file for tbl.
func GetTablePath(tbl string) (string, bool) {
	p := filepath.Join(DatabasePath, tbl)
	_, err := os.Stat(p)
	if err != nil {
		return "", false
	}

	return p, true
}

// GetTableWAL returns the first WAL file for the given table name tbl in the
// database path and returns it and a bool signifying if it was found.
//
// There should only be, at most, a single WAL file for any table as WAL files
// are removed after successful transaction commits.
// If an invalid WAL file was generated, the system will delete them later.
func GetTableWAL(tbl string) (string, bool) {
	files, err := GetDirContents(DatabasePath)
	if err != nil {
		return "", false
	}

	for _, f := range files {
		if strings.Contains(f, tbl) && strings.HasSuffix(f, ".log") {
			return f, true
		}
	}

	return "", false
}

// -------Generic Utils---------------------------------------------------------

// GetDirContents returns all items in path or encountered error.
func GetDirContents(path string) ([]string, error) {
	var contents []string = []string{}

	items, err := os.ReadDir(path)
	if err != nil {
		return contents, err
	}

	for _, item := range items {
		entry := fmt.Sprintf("%s/%s", path, item.Name())
		contents = append(contents, entry)
	}

	return contents, nil
}

type PathLike interface {
	~string | *os.File
}

// GetStemFromString returns the file stem of a string path or *os.File.
// E.g. "V:/dir/file.txt" becomes "file".
func GetStem[T PathLike](v T) (string, error) {
	var base string
	switch x := any(v).(type) {

	case string:
		base = filepath.Base(x)

	case *os.File:
		temp, err := filepath.Abs(x.Name())
		if err != nil {
			return "", err
		}
		base = filepath.Base(temp)
	}

	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)

	return stem, nil
}
