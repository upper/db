package db

// Item provides methods for
type Item interface {
	Save(Session) error
	Delete(Session) error
	Update(Session, M) error
	Changes() M
}
