package globals

// -----------------------------------------------------------------------------
// Shared, or "global", constants that are referenced between packages.
// This is not meant to contain mutable values.
// -----------------------------------------------------------------------------

const (
	OP_SET byte = 1
	OP_DEL byte = 2
)

const (
	DATE_LAYOUT = "01-02-2006"  // MM-DD-YYYY
	TIME_LAYOUT = "15-04-05-00" // HH-MM-SS-XX
)

