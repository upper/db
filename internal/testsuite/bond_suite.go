package testsuite

import (
	"fmt"
	"log"
	"time"

	"database/sql"
	"github.com/stretchr/testify/suite"
	"github.com/upper/db"
	"github.com/upper/db/sqlbuilder"
)

type Session struct {
	sqlbuilder.Session
}

func (d *Session) AccountCollection() *AccountCollection {
	return NewAccountCollection(d.Session)
}

func (d *Session) UserCollection() *UserCollection {
	return NewUserCollection(d.Session)
}

func (d *Session) LogCollection() *LogCollection {
	return NewLogCollection(d.Session)
}

func NewSession(sess db.Session) *Session {
	return &Session{Session: sess.(sqlbuilder.Session)}
}

func NewAccountCollection(sess db.Session) *AccountCollection {
	return &AccountCollection{Collection: sess.Collection("accounts")}
}

func NewUserCollection(sess db.Session) *UserCollection {
	return &UserCollection{Collection: sess.Collection("users")}
}

func NewLogCollection(sess db.Session) *LogCollection {
	return &LogCollection{Collection: sess.Collection("logs")}
}

type Log struct {
	ID      uint64 `db:"id,omitempty"`
	Message string `db:"message"`
}

type Account struct {
	ID        uint64     `db:"id,omitempty"`
	Name      string     `db:"name"`
	Disabled  bool       `db:"disabled"`
	CreatedAt *time.Time `db:"created_at,omitempty"`
}

func (a *Account) Collection(sess db.Session) db.Collection {
	return sess.Collection("accounts")
}

func (a *Account) AfterCreate(sess db.Session) error {
	message := fmt.Sprintf("Account %q was created.", a.Name)
	return sess.Item(&Log{Message: message}).Save()
}

func (a *Account) BeforeDelete() error {
	// TODO: we should have flags on the object that we set here..
	// and easily reset.. for testing
	log.Println("beforedelete()..")
	return nil
}

type User struct {
	ID        uint64 `db:"id,omitempty"`
	AccountID uint64 `db:"account_id"`
	Username  string `db:"username"`
}

func (u *User) AfterCreate(sess db.Session) error {
	message := fmt.Sprintf("User %q was created.", u.Username)
	return sess.Item(&Log{Message: message}).Save()
}

func (u *User) Collection(sess db.Session) db.Collection {
	return sess.Collection("users")
}

func (l *Log) Collection(sess db.Session) db.Collection {
	return sess.Collection("logs")
}

type LogCollection struct {
	db.Collection
}

type AccountCollection struct {
	db.Collection
}

func (s AccountCollection) FindOne(cond db.Cond) (*Account, error) {
	var a *Account
	err := s.Find(cond).One(&a)
	return a, err
}

type UserCollection struct {
	db.Collection
}

type BondTestSuite struct {
	sess *Session

	suite.Suite

	Helper
}

func (s *BondTestSuite) AfterTest(suiteName, testName string) {
	err := s.TearDown()
	s.NoError(err)
}

func (s *BondTestSuite) BeforeTest(suiteName, testName string) {
	err := s.TearUp()
	s.NoError(err)

	sess := s.Helper.Session().(sqlbuilder.Session)

	cols, _ := sess.Collections()
	for i := range cols {
		err = cols[i].Truncate()
		s.NoError(err)
	}

	s.sess = NewSession(sess)
}

func (s *BondTestSuite) Session() *Session {
	return s.sess
}

func (s *BondTestSuite) TestAccountCollection() {
	sess := s.Session()

	user := User{Username: "peter"}

	err := sess.Item(&user).Save()
	s.NoError(err)

	err = sess.Item(&User{Username: "peter"}).Save()
	s.Error(err, "Should fail because user is a unique value")

	account1 := Account{Name: "Pressly"}
	err = sess.Item(&account1).Save()
	s.NoError(err)

	account2 := &Account{}

	err = sess.AccountCollection().
		Find(account1.ID).
		One(&account2)

	s.NoError(err)
	s.Equal(account1.Name, account2.Name)

	var account3 Account
	err = sess.Collection("accounts").
		Find(account1.ID).
		One(&account3)

	s.NoError(err)
	s.Equal(account1.Name, account3.Name)

	colName := sess.Collection("accounts").Name()
	s.Equal("accounts", colName)

	count, err := sess.AccountCollection().
		Find(db.Cond{}).
		Count()
	s.NoError(err)
	s.True(count == 1)

	count, err = sess.AccountCollection().
		Find().
		Count()
	s.NoError(err)
	s.True(count == 1)

	var a Account
	err = sess.AccountCollection().
		Find(db.Cond{"id": account1.ID}).One(&a)
	s.NoError(err)
	s.NotNil(a)

	account1.Disabled = true
	err = sess.Item(&account1).Save()
	s.NoError(err)

	count, err = sess.AccountCollection().
		Find(db.Cond{}).
		Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	err = sess.Item(&account1).Delete()
	s.NoError(err)

	count, err = sess.AccountCollection().Find().Count()
	s.NoError(err)
	s.Zero(count)
}

func (s *BondTestSuite) TestDelete() {
	sess := s.Session()

	account := Account{Name: "Pressly"}
	err := sess.Item(&account).
		Save()
	s.NoError(err)
	s.NotZero(account.ID)

	// Delete by query -- without callbacks
	err = sess.AccountCollection().
		Find(account.ID).
		Delete()
	s.NoError(err)

	count, err := sess.AccountCollection().
		Find(account.ID).
		Count()
	s.Zero(count)
}

func (s *BondTestSuite) TestSlices() {
	sess := s.Session()

	id, err := sess.AccountCollection().
		Insert(&Account{Name: "Apple"})
	s.NoError(err)
	s.NotZero(id)

	id, err = sess.AccountCollection().
		Insert(Account{Name: "Google"})
	s.NoError(err)
	s.NotZero(id)

	var accounts []*Account
	err = sess.AccountCollection().
		Find(db.Cond{}).
		All(&accounts)
	s.NoError(err)
	s.Len(accounts, 2)
}

func (s *BondTestSuite) TestSelectOnlyIDs() {
	sess := s.Session()

	id, err := sess.AccountCollection().
		Insert(&Account{Name: "Apple"})
	s.NoError(err)
	s.NotZero(id)

	id, err = sess.AccountCollection().
		Insert(Account{Name: "Google"})
	s.NoError(err)
	s.NotZero(id)

	var ids []struct {
		Id int64 `db:"id"`
	}

	err = sess.AccountCollection().
		Find(db.Cond{}).
		Select("id").All(&ids)
	s.NoError(err)
	s.Len(ids, 2)
	s.NotEmpty(ids[0])
}

func (s *BondTestSuite) TestTx() {
	sess := s.Session()

	user := User{Username: "peter"}
	err := sess.Item(&user).Save()
	s.NoError(err)

	// This transaction should fail because user is a UNIQUE value and we already
	// have a "peter".
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		return tx.Item(&User{Username: "peter"}).Save()
	})
	s.Error(err)

	// This transaction should fail because user is a UNIQUE value and we already
	// have a "peter".
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		return tx.Item(&User{Username: "peter"}).Save()
	})
	s.Error(err)

	// This transaction will have no errors, but we'll produce one in order for
	// it to rollback at the last moment.
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		if err := tx.Item(&User{Username: "Joe"}).Save(); err != nil {
			return err
		}

		if err := tx.Item(&User{Username: "Cool"}).Save(); err != nil {
			return err
		}

		return fmt.Errorf("Rolling back for no reason.")
	})
	s.Error(err)

	// Attempt to add two new unique values, if the transaction above had not
	// been rolled back this transaction will fail.
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		if err := tx.Item(&User{Username: "Joe"}).Save(); err != nil {
			return err
		}

		if err := tx.Item(&User{Username: "Cool"}).Save(); err != nil {
			return err
		}

		return nil
	})
	s.NoError(err)

	// If the transaction above was successful, this one will fail.
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		if err := tx.Item(&User{Username: "Joe"}).Save(); err != nil {
			return err
		}

		if err := tx.Item(&User{Username: "Cool"}).Save(); err != nil {
			return err
		}

		return nil
	})
	s.Error(err)
}

func (s *BondTestSuite) TestInheritedTx() {
	sess := s.Session()

	sqlDB := sess.Driver().(*sql.DB)

	user := User{Username: "peter"}
	err := sess.Item(&user).Save()
	s.NoError(err)

	// Create a transaction
	sqlTx, err := sqlDB.Begin()
	s.NoError(err)

	// And pass that transaction to bond, this whole session is a transaction.
	bondTx, err := sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Should fail because user is a UNIQUE value and we already have a "peter".
	err = bondTx.Item(&User{Username: "peter"}).Save()
	s.Error(err)

	// The transaction is controlled outside bond.
	err = sqlTx.Rollback()
	s.NoError(err)

	// The sqlTx is worthless now.
	err = bondTx.Item(&User{Username: "peter-2"}).Save()
	s.Error(err)

	// But we can create a new one.
	sqlTx, err = sqlDB.Begin()
	s.NoError(err)
	s.NotNil(sqlTx)

	// And create another bond session.
	bondTx, err = sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Adding two new values.
	err = bondTx.Item(&User{Username: "Joe-2"}).Save()
	s.NoError(err)

	err = bondTx.Item(&User{Username: "Cool-2"}).Save()
	s.NoError(err)

	// And a value that is going to be rolled back.
	err = bondTx.Item(&Account{Name: "Rolled back"}).Save()
	s.NoError(err)

	// This session happens to be a transaction, let's rollback everything.
	err = sqlTx.Rollback()
	s.NoError(err)

	// Start again.
	sqlTx, err = sqlDB.Begin()
	s.NoError(err)

	tx, err := sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Attempt to add two unique values.
	err = tx.Item(&User{Username: "Joe-2"}).Save()
	s.NoError(err)

	err = tx.Item(&User{Username: "Cool-2"}).Save()
	s.NoError(err)

	// And a value that is going to be commited.
	err = tx.Item(&Account{Name: "Commited!"}).Save()
	s.NoError(err)

	// Yes, commit them.
	err = sqlTx.Commit()
	s.NoError(err)
}

func (s *BondTestSuite) TestUnknownCollection() {
	var err error

	sess := s.Session()

	err = sess.Item(nil).Save()
	s.Error(err)

	_, err = sess.Collection("users").Insert(&User{Username: "Foo"})
	s.NoError(err)
}
