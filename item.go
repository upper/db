package db

// Item defines additonal methods for db.Model objects.
type Item interface {
	Save(Session) error
	Delete(Session) error
	Update(Session, M) error
	Changes() M
}
