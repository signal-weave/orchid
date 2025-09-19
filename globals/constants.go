package globals

// -----------------------------------------------------------------------------
// Shared, or "global", constants that are referenced between packages.
// This is not meant to contain mutable values.
// -----------------------------------------------------------------------------

// -------Storage---------------------------------------------------------------

const (
	NodeHeaderSize = 3
	PageNumSize    = 8 // The size of a page's number in bytes

	// -------Page Marker-------------------------------------------------------

	PageMarkerSize = 4 // The size of a page marker in bytes

	PM_Z = byte('z')
	PM_T = byte('t')
	PM_C = byte('c')
	PM_H = byte('h')

	// -------WAL Marker--------------------------------------------------------

	WalMarkerSize = 4 // The size of a write-ahead-log marker in bytes

	WM_E = byte('E')
	WM_X = byte('X')
	WM_I = byte('I')
	WM_T = byte('T')
)

// Although not a constant, PageMarker is an array of constants and is orchid's
// magic marker page header value.
//
// A 'magic marker' is 4 bytes that only orchid db pages should start with.
// It would be a huge coincidence if another program wrote orchid's byte marker
// at exactly the offset orchid is reading from.
var PageMarker = []byte{PM_Z, PM_T, PM_C, PM_H}

// Although not a constant, WalSuccessMarker is an array of constants and is
// orchidd's successful write-ahead-log marker.
//
// If this value is not found at the end of a write-head-log (WAL) file, then an
// unexpected shutdown happened durring WAL creation, and the log should be
// ignored.
//
// Atomicity does not ensure writes `will` happen, it ensures that writes will
// either fully happen or not at all.
//
// If a WAL does not contain a marker, then the transaction is discarded
// entirely as we then cannot know what state the user intenteded to update the
// database to.
var WalSuccessMarker = []byte{WM_E, WM_X, WM_I, WM_T}

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
	WAL_SUFFIX = ".wal"
)

// -------Terminal--------------------------------------------------------------

const (
	DEFAULT_TERMINAL_W = 80
	DEFAULT_TERMINAL_H = 25
)
