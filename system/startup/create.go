package startup

import "os"

func CreateDatabaseDirectory(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(path, 0777)
			return nil
		}
		return err
	}

	return nil
}
