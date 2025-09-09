package system

import (
	"fmt"
	"os"

	"orchiddb/globals"
)

func Startup() error {
	err := makeDirs()
	if err != nil {
		return err
	}
	return nil
}

func makeDirs() error {
	err := os.MkdirAll(globals.FlushDir, 0o777)
	if err != nil {
		return fmt.Errorf("error creating dir: %v", err)
	}
	return nil
}
