package startup

import (
	"flag"
	"fmt"
	"os"

	"orchiddb/globals"
	"orchiddb/paths"
)

func parseCLIArgs(argv []string) error {
	// ! REMEMBER TO UPDATE fs.Usage() STRING WHEN ADDING OR REMOVING CLI VARS !

	fs := flag.NewFlagSet("runtime", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	dbPathHelp := "Path to place database files. Ideally is empty directory."
	fs.StringVar(&paths.DatabasePath, "path", paths.DatabasePath, dbPathHelp)

	addrHelp := "Which address the server uses for listening. Defaults to 127.0.0.1."
	fs.StringVar(&globals.Address, "addr", globals.Address, addrHelp)

	portHelp := "Which port the server uses for listening. Defaults to 6000"
	fs.IntVar(&globals.Port, "port", globals.Port, portHelp)

	pageHelp := "Size in bytes for a single database page. Defaults to OS page size."
	fs.IntVar(&globals.PageSize, "page-size", globals.PageSize, pageHelp)

	minHelp := "Minimum percentage a node must be filled to before consolidation."
	temp := fs.Float64("node-min", float64(globals.MinFillPercent), minHelp)
	globals.MinFillPercent = float32(*temp)

	maxHelp := "Maximum percentage a node must be to before splitting."
	temp = fs.Float64("node-max", float64(globals.MaxFillPercent), maxHelp)
	globals.MaxFillPercent = float32(*temp)

	const usageString = `Orchid runtime options:
	
  -path      string   Path to place database files. Ideally is empty directory.
  -addr      string   Which address the server uses for listening. Defaults to 127.0.0.1.
  -port      int      Which port the server usese for listening. Defaults to 6000.
  -page-size int      Size in bytes for a single database page. Defaults to OS page size.
  -node-min  float32  Minimum percentage a node must be filled to before consolidation.
  -node-max  float32  Maximum percentage a node must be to before splitting.
`
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usageString)
	}

	if err := fs.Parse(argv); err != nil {
		return err
	}

	return nil
}
