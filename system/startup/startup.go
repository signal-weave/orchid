package startup

import (
	"fmt"
	"os"
)

func Startup(argv []string) {
	parseCLI(argv)
	performRecoveryCheck()
}

// Parses and stores the runtime flags in public vars.
func parseCLI(argv []string) {
	if err := parseCLIArgs(argv); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
