package startup

import "os"

func CreateDatabaseDirectory(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if closeErr := os.MkdirAll(path, 0777); closeErr != nil {
				return closeErr
			}
			return nil
		}
		return err
	}

	return nil
}
