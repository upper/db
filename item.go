package db

// Item provides methods for
type Item interface {
	Save() error
	Delete() error
	Update(M) error
	Changes() M
}
