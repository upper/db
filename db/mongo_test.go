package db

import (
  //. "github.com/xiam/gosexy"
  "testing"
)

func TestConnect(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "dns.fail", Database: "test" })

  err := db.Connect()

  if err != nil {
    t.Logf("Got %t, this was intended.", err)
    return
  }

  t.Error("Are you serious?")
}

func TestAppend(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  /* Choose database */
  db.Use("test")

  /* Choose collection */
  col := db.Collection("people")
  
  /* Trying to get 10 records */
  all := col.FindAll(Limit(10))

  for i := 0; i < len(all); i++ {
    t.Logf("item[%t]\n", i)
    for key, val := range all[i] {
      t.Logf("\t%t: %t\n", key, val)
    }
  }
}
