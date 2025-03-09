package mongo_test

import (
	"context"
	"fmt"

	db "github.com/upper/db/v4"
	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"github.com/upper/db/v4/adapter/mongo"
)

type Helper struct {
	sess db.Session

	connURL mongo.ConnectionURL
}

func (h *Helper) Session() db.Session {
	return h.sess
}

func (h *Helper) Adapter() string {
	return "mongo"
}

func (h *Helper) TearDown() error {
	return h.sess.Close()
}

func (h *Helper) SetUp() error {
	ctx := context.Background()

	var err error

	h.sess, err = mongo.Open(h.connURL)
	if err != nil {
		return fmt.Errorf("mongo.Open: %w", err)
	}

	mgdb, ok := h.sess.Driver().(*mongodrv.Client)
	if !ok {
		panic("expecting *mongo.Client")
	}

	var col *mongodrv.Collection
	col = mgdb.Database(h.connURL.Database).Collection("birthdays")
	_ = col.Drop(ctx)

	col = mgdb.Database(h.connURL.Database).Collection("fibonacci")
	_ = col.Drop(ctx)

	col = mgdb.Database(h.connURL.Database).Collection("is_even")
	_ = col.Drop(ctx)

	col = mgdb.Database(h.connURL.Database).Collection("CaSe_TesT")
	_ = col.Drop(ctx)

	// Getting a pointer to the "artist" collection.
	artist := h.sess.Collection("artist")

	_ = artist.Truncate()

	/*
		for i := 0; i < 999; i++ {
			_, err = artist.Insert(artistType{
				Name: fmt.Sprintf("artist-%d", i),
			})
			if err != nil {
				return fmt.Errorf("insert: %w", err)
			}
		}
	*/

	return nil
}

func NewHelper(connURL mongo.ConnectionURL) *Helper {
	return &Helper{
		connURL: connURL,
	}
}
