package testsuite

import (
	"fmt"
	"time"

	"database/sql"

	"github.com/stretchr/testify/suite"
	"github.com/upper/db/v4"
	"github.com/upper/db/v4/sqlbuilder"
)

type Session struct {
	sqlbuilder.Session
}

func (d *Session) Accounts() *Accounts {
	return NewAccounts(d.Session)
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

func NewAccounts(sess db.Session) *Accounts {
	return &Accounts{Collection: sess.Collection("accounts")}
}

func NewUserCollection(sess db.Session) *UserCollection {
	return &UserCollection{Collection: sess.Collection("users")}
}

func NewLogCollection(sess db.Session) *LogCollection {
	return &LogCollection{Collection: sess.Collection("logs")}
}

type Log struct {
	sqlbuilder.Item

	ID      uint64 `db:"id,omitempty"`
	Message string `db:"message"`
}

type Account struct {
	sqlbuilder.Item

	ID        uint64     `db:"id,omitempty"`
	Name      string     `db:"name"`
	Disabled  bool       `db:"disabled"`
	CreatedAt *time.Time `db:"created_at,omitempty"`
}

func (account *Account) Collection(sess db.Session) db.Collection {
	return sess.Collection("accounts")
}

func (account *Account) AfterCreate(sess db.Session) error {
	message := fmt.Sprintf("Account %q was created.", account.Name)
	return sess.Save(&Log{Message: message})
}

type User struct {
	sqlbuilder.Item

	ID        uint64 `db:"id,omitempty"`
	AccountID uint64 `db:"account_id"`
	Username  string `db:"username"`
}

func (user *User) AfterCreate(sess db.Session) error {
	message := fmt.Sprintf("User %q was created.", user.Username)
	return sess.Save(&Log{Message: message})
}

func (user *User) Collection(sess db.Session) db.Collection {
	return sess.Collection("users")
}

func (l *Log) Collection(sess db.Session) db.Collection {
	return sess.Collection("logs")
}

type LogCollection struct {
	db.Collection
}

type Accounts struct {
	db.Collection
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

func (s *BondTestSuite) TestFindOne() {
	var err error
	sess := s.Session()

	user := User{Username: "jose"}
	err = sess.Save(&user)
	s.NoError(err)

	s.NotZero(user.ID)

	s.Equal(db.M{}, user.Changes())
	user.Username = "jose-2"
	s.Equal(db.M{"username": "jose-2"}, user.Changes())

	user = User{}
	err = sess.Get(&user, db.Cond{"username": "jose"})

	s.NoError(err)

	s.Equal(db.M{}, user.Changes())
	user.Username = "catalina"
	s.Equal(db.M{"username": "catalina"}, user.Changes())

	err = sess.Save(&user)
	s.NoError(err)

	s.Equal(db.M{}, user.Changes())
	user.Username = "nala"
	s.Equal(db.M{"username": "nala"}, user.Changes())

	err = sess.Save(&user)
	s.NoError(err)
	s.Equal(db.M{}, user.Changes())

	userID := user.ID

	user = User{}
	err = sess.Get(&user, userID)

	s.Equal("nala", user.Username)

	user = User{}
	err = sess.Get(&user, userID)
	s.NoError(err)
	s.Equal("nala", user.Username)

	err = user.Delete(sess)
	s.NoError(err)

	err = sess.Get(&user, userID)
	s.Error(err)

	err = sess.Collection("users").
		Find(userID).
		One(&user)
	s.Error(err)
}

func (s *BondTestSuite) TestAccounts() {
	sess := s.Session()

	user := User{Username: "peter"}

	err := sess.Save(&user)
	s.NoError(err)

	user = User{Username: "peter"}
	err = sess.Save(&user)
	s.Error(err, "username should be unique")

	account1 := Account{Name: "Pressly"}
	err = sess.Save(&account1)
	s.NoError(err)

	account2 := Account{}
	err = sess.Get(&account2, account1.ID)

	s.NoError(err)
	s.Equal(account1.Name, account2.Name)

	var account3 Account
	err = sess.Get(&account3, account1.ID)

	s.NoError(err)
	s.Equal(account1.Name, account3.Name)

	var a Account
	err = sess.Get(&a, account1.ID)
	s.NoError(err)
	s.NotNil(a)

	account1.Disabled = true
	err = sess.Save(&account1)
	s.NoError(err)

	count, err := sess.Accounts().Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	err = account1.Delete(sess)
	s.NoError(err)

	count, err = sess.Accounts().Find().Count()
	s.NoError(err)
	s.Zero(count)
}

func (s *BondTestSuite) TestDelete() {
	sess := s.Session()

	account := Account{Name: "Pressly"}
	err := sess.Save(&account)
	s.NoError(err)
	s.NotZero(account.ID)

	// Delete by query -- without callbacks
	err = sess.Accounts().
		Find(account.ID).
		Delete()
	s.NoError(err)

	count, err := sess.Accounts().Find(account.ID).Count()
	s.Zero(count)
}

func (s *BondTestSuite) TestSlices() {
	sess := s.Session()

	err := sess.Save(&Account{Name: "Apple"})
	s.NoError(err)

	err = sess.Save(&Account{Name: "Google"})
	s.NoError(err)

	var accounts []*Account
	err = sess.Accounts().
		Find(db.Cond{}).
		All(&accounts)
	s.NoError(err)
	s.Len(accounts, 2)
}

func (s *BondTestSuite) TestSelectOnlyIDs() {
	sess := s.Session()

	err := sess.Save(&Account{Name: "Apple"})
	s.NoError(err)

	err = sess.Save(&Account{Name: "Google"})
	s.NoError(err)

	var ids []struct {
		Id int64 `db:"id"`
	}

	err = sess.Accounts().
		Find().
		Select("id").All(&ids)
	s.NoError(err)
	s.Len(ids, 2)
	s.NotEmpty(ids[0])
}

func (s *BondTestSuite) TestTx() {
	sess := s.Session()

	user := User{Username: "peter"}
	err := sess.Save(&user)
	s.NoError(err)

	// This transaction should fail because user is a UNIQUE value and we already
	// have a "peter".
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		return tx.Save(&User{Username: "peter"})
	})
	s.Error(err)

	// This transaction should fail because user is a UNIQUE value and we already
	// have a "peter".
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		return tx.Save(&User{Username: "peter"})
	})
	s.Error(err)

	// This transaction will have no errors, but we'll produce one in order for
	// it to rollback at the last moment.
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		if err := tx.Save(&User{Username: "Joe"}); err != nil {
			return err
		}

		if err := tx.Save(&User{Username: "Cool"}); err != nil {
			return err
		}

		return fmt.Errorf("Rolling back for no reason.")
	})
	s.Error(err)

	// Attempt to add two new unique values, if the transaction above had not
	// been rolled back this transaction will fail.
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		if err := tx.Save(&User{Username: "Joe"}); err != nil {
			return err
		}

		if err := tx.Save(&User{Username: "Cool"}); err != nil {
			return err
		}

		return nil
	})
	s.NoError(err)

	// If the transaction above was successful, this one will fail.
	err = sess.Tx(func(tx sqlbuilder.Tx) error {
		if err := tx.Save(&User{Username: "Joe"}); err != nil {
			return err
		}

		if err := tx.Save(&User{Username: "Cool"}); err != nil {
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
	err := sess.Save(&user)
	s.NoError(err)

	// Create a transaction
	sqlTx, err := sqlDB.Begin()
	s.NoError(err)

	// And pass that transaction to bond, this whole session is a transaction.
	bondTx, err := sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Should fail because user is a UNIQUE value and we already have a "peter".
	err = bondTx.Save(&User{Username: "peter"})
	s.Error(err)

	// The transaction is controlled outside bond.
	err = sqlTx.Rollback()
	s.NoError(err)

	// The sqlTx is worthless now.
	err = bondTx.Save(&User{Username: "peter-2"})
	s.Error(err)

	// But we can create a new one.
	sqlTx, err = sqlDB.Begin()
	s.NoError(err)
	s.NotNil(sqlTx)

	// And create another bond session.
	bondTx, err = sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Adding two new values.
	err = bondTx.Save(&User{Username: "Joe-2"})
	s.NoError(err)

	err = bondTx.Save(&User{Username: "Cool-2"})
	s.NoError(err)

	// And a value that is going to be rolled back.
	err = bondTx.Save(&Account{Name: "Rolled back"})
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
	err = tx.Save(&User{Username: "Joe-2"})
	s.NoError(err)

	err = tx.Save(&User{Username: "Cool-2"})
	s.NoError(err)

	// And a value that is going to be commited.
	err = tx.Save(&Account{Name: "Commited!"})
	s.NoError(err)

	// Yes, commit them.
	err = sqlTx.Commit()
	s.NoError(err)
}

func (s *BondTestSuite) TestUnknownCollection() {
	var err error

	sess := s.Session()

	err = sess.Save(nil)
	s.Error(err)

	_, err = sess.Collection("users").Insert(&User{Username: "Foo"})
	s.NoError(err)
}
