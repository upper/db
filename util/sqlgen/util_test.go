package sqlgen

import (
	"regexp"
	"strings"
)

var (
	reInvisible = regexp.MustCompile(`[\t\n\r]`)
	reSpace     = regexp.MustCompile(`\s+`)
)

func trim(a string) string {
	a = reInvisible.ReplaceAllString(strings.TrimSpace(a), " ")
	a = reSpace.ReplaceAllString(strings.TrimSpace(a), " ")
	return a
}
