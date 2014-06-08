package postgresql

type Tx struct {
	*Source
}

func (self *Tx) Commit() error {
	return self.Source.tx.Commit()
}

func (self *Tx) Rollback() error {
	return self.Source.tx.Rollback()
}
