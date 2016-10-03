package db

import (
	"os"
)

func envEnabled(name string) bool {
	switch os.Getenv(name) {
	case "1", "true", "TRUE", "t", "T":
		return true
	}
	return false
}
