package globals

// -----------------------------------------------------------------------------
// Shared, or "global", constants that are referenced between packages.
// This is not meant to contain mutable values.
// -----------------------------------------------------------------------------

// -------Storage---------------------------------------------------------------
const (
	NodeHeaderSize = 3
	PageNumSize    = 8 // The size of a page's number in bytes

	PageMarkerSize = 4 // The size of a page marker in bytes
	
	PM_Z = byte('z')
	PM_T = byte('t')
	PM_C = byte('c')
	PM_H = byte('h')
)

var PageMarker = []byte{PM_Z, PM_T, PM_C, PM_H}

// -------Misc------------------------------------------------------------------
const (
	Developer = "Signal Weave"
)

const (
	DATE_LAYOUT      = "01-02-2006"             // MM-DD-YYYY
	TIME_LAYOUT      = "15-04-05-00"            // HH-MM-SS-XX
	DATE_TIME_LAYOUT = "01-02-2006-15-04-05-00" // MM-DD-YYYY-HH-MM-SS-XX
)

// -------Filenames-------------------------------------------------------------

const (
	MANIFEST_FILE = "_manifest.json"
)

// -------Terminal--------------------------------------------------------------

const (
	DEFAULT_TERMINAL_W = 80
	DEFAULT_TERMINAL_H = 25
)
