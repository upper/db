package testsuite

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/upper/db/v4"
	"github.com/upper/db/v4/sqlbuilder"
)

func Accounts(sess db.Session) db.Collection {
	return sess.Collection("accounts")
}

func Users(sess db.Session) db.Collection {
	return sess.Collection("users")
}

func Logs(sess db.Session) db.Collection {
	return sess.Collection("logs")
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

func (*Account) Collection(sess db.Session) db.Collection {
	return Accounts(sess)
}

func (a *Account) AfterCreate(sess db.Session) error {
	message := fmt.Sprintf("Account %q was created.", a.Name)
	return sess.Save(&Log{Message: message})
}

type User struct {
	ID        uint64 `db:"id,omitempty"`
	AccountID uint64 `db:"account_id"`
	Username  string `db:"username"`
}

func (u *User) AfterCreate(sess db.Session) error {
	message := fmt.Sprintf("User %q was created.", u.Username)
	return sess.Save(&Log{Message: message})
}

func (*User) Collection(sess db.Session) db.Collection {
	return Users(sess)
}

func (*Log) Collection(sess db.Session) db.Collection {
	return Logs(sess)
}

type RecordTestSuite struct {
	suite.Suite
	Helper
}

func (s *RecordTestSuite) AfterTest(suiteName, testName string) {
	err := s.TearDown()
	s.NoError(err)
}

func (s *RecordTestSuite) BeforeTest(suiteName, testName string) {
	err := s.TearUp()
	s.NoError(err)

	sess := s.Helper.Session().(sqlbuilder.Session)

	cols, err := sess.Collections()
	s.NoError(err)

	for i := range cols {
		err = cols[i].Truncate()
		s.NoError(err)
	}
}

func (s *RecordTestSuite) TestFindOne() {
	var err error
	sess := s.Session()

	user := User{Username: "jose"}
	err = sess.Save(&user)
	s.NoError(err)

	s.NotZero(user.ID)
	userID := user.ID

	user = User{}
	err = Users(sess).Find(userID).One(&user)
	s.NoError(err)
	s.Equal("jose", user.Username)

	user = User{}
	err = sess.Get(&user, db.Cond{"username": "jose"})
	s.NoError(err)
	s.Equal("jose", user.Username)

	user.Username = "Catalina"
	err = sess.Save(&user)
	s.NoError(err)

	user = User{}
	err = sess.Get(&user, userID)
	s.NoError(err)
	s.Equal("Catalina", user.Username)

	err = sess.Delete(&user)
	s.NoError(err)

	err = sess.Get(&user, userID)
	s.Error(err)

	err = sess.Collection("users").
		Find(userID).
		One(&user)
	s.Error(err)
}

func (s *RecordTestSuite) TestAccounts() {
	sess := s.Session()

	user := User{Username: "peter"}

	err := sess.Save(&user)
	s.NoError(err)

	user = User{Username: "peter"}
	err = sess.Save(&user)
	s.Error(err, "username should be unique")

	account1 := Account{Name: "skywalker"}
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

	count, err := Accounts(sess).Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	err = sess.Delete(&account1)
	s.NoError(err)

	count, err = Accounts(sess).Find().Count()
	s.NoError(err)
	s.Zero(count)
}

func (s *RecordTestSuite) TestDelete() {
	sess := s.Session()

	account := Account{Name: "Pressly"}
	err := sess.Save(&account)
	s.NoError(err)
	s.NotZero(account.ID)

	// Delete by query -- without callbacks
	err = Accounts(sess).
		Find(account.ID).
		Delete()
	s.NoError(err)

	count, err := Accounts(sess).Find(account.ID).Count()
	s.Zero(count)
}

func (s *RecordTestSuite) TestSlices() {
	sess := s.Session()

	err := sess.Save(&Account{Name: "Apple"})
	s.NoError(err)

	err = sess.Save(&Account{Name: "Google"})
	s.NoError(err)

	var accounts []*Account
	err = Accounts(sess).
		Find(db.Cond{}).
		All(&accounts)
	s.NoError(err)
	s.Len(accounts, 2)
}

func (s *RecordTestSuite) TestSelectOnlyIDs() {
	sess := s.Session()

	err := sess.Save(&Account{Name: "Apple"})
	s.NoError(err)

	err = sess.Save(&Account{Name: "Google"})
	s.NoError(err)

	var ids []struct {
		Id int64 `db:"id"`
	}

	err = Accounts(sess).
		Find().
		Select("id").All(&ids)
	s.NoError(err)
	s.Len(ids, 2)
	s.NotEmpty(ids[0])
}

func (s *RecordTestSuite) TestTx() {
	sess := s.Session().(sqlbuilder.Session)

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

func (s *RecordTestSuite) TestInheritedTx() {
	sess := s.Session()

	sqlDB := sess.Driver().(*sql.DB)

	user := User{Username: "peter"}
	err := sess.Save(&user)
	s.NoError(err)

	// Create a transaction
	sqlTx, err := sqlDB.Begin()
	s.NoError(err)

	// And pass that transaction to upper/db, this whole session is a transaction.
	upperTx, err := sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Should fail because user is a UNIQUE value and we already have a "peter".
	err = upperTx.Save(&User{Username: "peter"})
	s.Error(err)

	// The transaction is controlled outside upper/db.
	err = sqlTx.Rollback()
	s.NoError(err)

	// The sqlTx is worthless now.
	err = upperTx.Save(&User{Username: "peter-2"})
	s.Error(err)

	// But we can create a new one.
	sqlTx, err = sqlDB.Begin()
	s.NoError(err)
	s.NotNil(sqlTx)

	// And create another session.
	upperTx, err = sqlbuilder.BindTx(s.Adapter(), sqlTx)
	s.NoError(err)

	// Adding two new values.
	err = upperTx.Save(&User{Username: "Joe-2"})
	s.NoError(err)

	err = upperTx.Save(&User{Username: "Cool-2"})
	s.NoError(err)

	// And a value that is going to be rolled back.
	err = upperTx.Save(&Account{Name: "Rolled back"})
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

func (s *RecordTestSuite) TestUnknownCollection() {
	var err error
	sess := s.Session()

	err = sess.Save(nil)
	s.Error(err)

	_, err = sess.Collection("users").Insert(&User{Username: "Foo"})
	s.NoError(err)
}
