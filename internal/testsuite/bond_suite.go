package testsuite

import (
	"fmt"
	"log"
	"time"

	"database/sql"
	"github.com/stretchr/testify/suite"
	"github.com/upper/db"
	"github.com/upper/db/bond"
	"github.com/upper/db/sqlbuilder"
)

type BondSession struct {
	bond.Session
}

func (d *BondSession) AccountStore() *AccountStore {
	return NewAccountStore(d.Session)
}

func (d *BondSession) UserStore() *UserStore {
	return NewUserStore(d.Session)
}

func (d *BondSession) LogStore() *LogStore {
	return NewLogStore(d.Session)
}

func NewBond(sess bond.Engine) *BondSession {
	return &BondSession{Session: bond.New(sess)}
}

func NewAccountStore(sess bond.Session) *AccountStore {
	return &AccountStore{Store: sess.Store("accounts")}
}

func NewUserStore(sess bond.Session) *UserStore {
	return &UserStore{Store: sess.Store("users")}
}

func NewLogStore(sess bond.Session) *LogStore {
	return &LogStore{Store: sess.Store("logs")}
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

func (a *Account) Store(sess bond.Session) bond.Store {
	return sess.Store("accounts")
}

func (a Account) AfterCreate(sess bond.Session) error {
	message := fmt.Sprintf("Account %q was created.", a.Name)
	return sess.Save(&Log{Message: message})
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

func (u User) AfterCreate(sess bond.Session) error {
	message := fmt.Sprintf("User %q was created.", u.Username)
	return sess.Save(&Log{Message: message})
}

func (u *User) Store(sess bond.Session) bond.Store {
	return sess.Store("users")
}

func (l *Log) Store(sess bond.Session) bond.Store {
	return sess.Store("logs")
}

type LogStore struct {
	bond.Store
}

type AccountStore struct {
	bond.Store
}

func (s AccountStore) FindOne(cond db.Cond) (*Account, error) {
	var a *Account
	err := s.Find(cond).One(&a)
	return a, err
}

type UserStore struct {
	bond.Store
}

type BondTestSuite struct {
	sess *BondSession

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

	sess := s.Helper.Session().(sqlbuilder.Database)

	cols, _ := sess.Collections()
	for _, k := range cols {
		_ = sess.Collection(k).Truncate()
	}

	s.sess = &BondSession{bond.New(sess)}
}

func (s *BondTestSuite) Session() *BondSession {
	return s.sess
}

func (s *BondTestSuite) TestAccountStore() {
	sess := s.Session()

	user := User{Username: "peter"}
	err := sess.Save(&user)
	s.NoError(err)

	err = sess.Save(&User{Username: "peter"})
	s.Error(err, "Should fail because user is a unique value")

	account1 := Account{Name: "Pressly"}
	err = sess.Save(&account1)
	s.NoError(err)

	account2 := &Account{}

	err = sess.AccountStore().Find(account1.ID).One(&account2)
	s.NoError(err)
	s.Equal(account1.Name, account2.Name)

	var account3 Account
	err = sess.Store(&account1).Find(account1.ID).One(&account3)
	s.NoError(err)
	s.Equal(account1.Name, account3.Name)

	colName := sess.Store("accounts").Name()
	s.Equal("accounts", colName)

	count, err := sess.AccountStore().Find(db.Cond{}).Count()
	s.NoError(err)
	s.True(count == 1)

	count, err = sess.AccountStore().Find().Count()
	s.NoError(err)
	s.True(count == 1)

	a, err := sess.AccountStore().FindOne(db.Cond{"id": account1.ID})
	s.NoError(err)
	s.NotNil(a)

	account1.Disabled = true
	err = sess.Save(&account1)
	s.NoError(err)

	count, err = sess.AccountStore().Find(db.Cond{}).Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	err = sess.Delete(&account1)
	s.NoError(err)

	count, err = sess.AccountStore().Find().Count()
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
	err = sess.AccountStore().Find(account.ID).Delete()
	s.NoError(err)

	count, err := sess.AccountStore().Find(account.ID).Count()
	s.Zero(count)
}

func (s *BondTestSuite) TestSlices() {
	sess := s.Session()

	id, err := sess.AccountStore().Insert(&Account{Name: "Apple"})
	s.NoError(err)
	s.NotZero(id)

	id, err = sess.AccountStore().Insert(Account{Name: "Google"})
	s.NoError(err)
	s.NotZero(id)

	var accounts []*Account
	err = sess.AccountStore().Find(db.Cond{}).All(&accounts)
	s.NoError(err)
	s.Len(accounts, 2)
}

func (s *BondTestSuite) TestSelectOnlyIDs() {
	sess := s.Session()

	id, err := sess.AccountStore().Insert(&Account{Name: "Apple"})
	s.NoError(err)
	s.NotZero(id)

	id, err = sess.AccountStore().Insert(Account{Name: "Google"})
	s.NoError(err)
	s.NotZero(id)

	var ids []struct {
		Id int64 `db:"id"`
	}

	err = sess.AccountStore().Find(db.Cond{}).Select("id").All(&ids)
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
	err = sess.Tx(func(tx bond.Session) error {
		return tx.Save(&User{Username: "peter"})
	})
	s.Error(err)

	// This transaction should fail because user is a UNIQUE value and we already
	// have a "peter".
	err = sess.Tx(func(tx bond.Session) error {
		return tx.Save(&User{Username: "peter"})
	})
	s.Error(err)

	// This transaction will have no errors, but we'll produce one in order for
	// it to rollback at the last moment.
	err = sess.Tx(func(tx bond.Session) error {
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
	err = sess.Tx(func(tx bond.Session) error {
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
	err = sess.Tx(func(tx bond.Session) error {
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
	bondTx, err := bond.Bind(s.Adapter(), sqlTx)
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
	bondTx, err = bond.Bind(s.Adapter(), sqlTx)
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

	bondTx, err = bond.Bind(s.Adapter(), sqlTx)
	s.NoError(err)

	// Attempt to add two unique values.
	err = bondTx.Save(&User{Username: "Joe-2"})
	s.NoError(err)

	err = bondTx.Save(&User{Username: "Cool-2"})
	s.NoError(err)

	// And a value that is going to be commited.
	err = bondTx.Save(&Account{Name: "Commited!"})
	s.NoError(err)

	// Yes, commit them.
	err = sqlTx.Commit()
	s.NoError(err)
}

func (s *BondTestSuite) TestUnknownStore() {
	var err error

	sess := s.Session()

	err = sess.Save(nil)
	s.Error(err)

	err = sess.Store(11).Save(&User{Username: "Foo"})
	s.Error(err)
}
