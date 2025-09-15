package filestamp

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// Capture a stable anchor at process start.
var (
	processStartWallUTC = time.Now().UTC() // wall clock snapshot
	processStartMono    = time.Now()       // carries monotonic reading
	seq                 uint64             // tie-breaker for same-ns events
)

// NowStable returns a time derived from the process-start wall time
// plus monotonic elapsed time. Immune to wall-clock jumps.
func NowStable() time.Time {
	elapsed := time.Since(processStartMono) // monotonic duration
	return processStartWallUTC.Add(elapsed)
}

// FileStamp returns a filename-safe timestamp like
// 2025-09-14T22-11-33.123456789Z-0001 derived from NowStable().
// The suffix prevents collisions within the same nanosecond.
func FileStamp() string {
	t := NowStable()

	// RFC3339Nano but replace ":" to "-" to be filesystem-friendly on Windows.
	iso := t.Format(time.RFC3339Nano) // e.g., 2025-09-14T22:11:33.123456789Z
	safe := make([]byte, len(iso))
	for i := 0; i < len(iso); i++ {
		if iso[i] == ':' {
			safe[i] = '-'
		} else {
			safe[i] = iso[i]
		}
	}

	// Tiebreaker for same-ns calls
	n := atomic.AddUint64(&seq, 1)
	return string(safe) + "-" + fmt.Sprintf("%04s", leftPad(strconv.FormatUint(n, 10), 4, '0'))
}

// FileName builds "<stem>_<stamp><suffix>" with the stable timestamp.
// e.g. ("FileName", ".log") will give
// FileName_2025-09-14T22-11-33.123456789Z-0001.log
func FileNameMonotonic(stem, suffix string) string {
	return fmt.Sprintf("%s_%s%s", stem, FileStamp(), extWithDot(suffix))
}

func extWithDot(ext string) string {
	if ext == "" {
		return ""
	}
	if ext[0] == '.' {
		return ext
	}
	return "." + ext
}

func leftPad(s string, width int, pad byte) string {
	if len(s) >= width {
		return s
	}
	b := make([]byte, width)
	for i := 0; i < width-len(s); i++ {
		b[i] = pad
	}
	copy(b[width-len(s):], s)
	return string(b)
}
