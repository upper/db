package sqlgen

import (
	"strings"
)

const (
	stageExpect = iota
	stageCapture
	stageClose
)

func isSpace(in byte) bool {
	return in == ' ' || in == '\t' || in == '\r' || in == '\n'
}

func trimString(in string) string {

	start, end := 0, len(in)-1

	// Where do we start cutting?
	for ; start <= end; start++ {
		if isSpace(in[start]) == false {
			break
		}
	}

	// Where do we end cutting?
	for ; end >= start; end-- {
		if isSpace(in[end]) == false {
			break
		}
	}

	return in[start : end+1]
}

func trimByte(in []byte) []byte {

	start, end := 0, len(in)-1

	// Where do we start cutting?
	for ; start <= end; start++ {
		if isSpace(in[start]) == false {
			break
		}
	}

	// Where do we end cutting?
	for ; end >= start; end-- {
		if isSpace(in[end]) == false {
			break
		}
	}

	return in[start : end+1]
}

/*
// Separates by a comma, ignoring spaces too.
// This was slower than strings.Split.
func separateByComma(in string) (out []string) {

	out = []string{}

	start, lim := 0, len(in)-1

	for start < lim {
		var end int

		for end = start; end <= lim; end++ {
			// Is a comma?
			if in[end] == ',' {
				break
			}
		}

		out = append(out, trimString(in[start:end]))

		start = end + 1
	}

	return
}
*/

// Separates by a comma, ignoring spaces too.
func separateByComma(in string) (out []string) {
	out = strings.Split(in, ",")
	for i := range out {
		out[i] = trimString(out[i])
	}
	return
}

// Separates by spaces, ignoring spaces too.
func separateBySpace(in string) (out []string) {
	l := len(in)

	if l == 0 {
		return []string{""}
	}

	out = make([]string, 0, l)

	pre := strings.Split(in, " ")

	for i := range pre {
		pre[i] = trimString(pre[i])
		if pre[i] != "" {
			out = append(out, pre[i])
		}
	}

	return
}

func separateByAS(in string) (out []string) {
	out = []string{}

	if len(in) < 6 {
		// Min expression: "a AS b"
		return []string{in}
	}

	start, lim := 0, len(in)-1

	for start <= lim {
		var end int

		for end = start; end <= lim; end++ {
			if end > 3 && isSpace(in[end]) && isSpace(in[end-3]) {
				if (in[end-1] == 's' || in[end-1] == 'S') && (in[end-2] == 'a' || in[end-2] == 'A') {
					break
				}
			}
		}

		if end < lim {
			out = append(out, trimString(in[start:end-3]))
		} else {
			out = append(out, trimString(in[start:end]))
		}

		start = end + 1
	}

	return
}
