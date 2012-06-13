package db

import (
  //. "github.com/xiam/gosexy"
  "fmt"
  "testing"
)

func TestAll(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }
 
  // Choose database
  db.Use("test")
  
  // Choose collection
  col := db.Collection("people")

  all := col.FindAll()

  for i := 0; i < len(all); i++ {
    for key, val := range all[i] {
      fmt.Printf("key(%v) = (%v)\n", key, val)
    }
  }

  /*
  // Testing insert
  col.Append(Tuple { "Name": "Tucket11", "LastName": "Nancy" })
  
  found := col.Find(
    Where { "Name": "Tucket3" },
    Where { "LastName $ne": "Barr" },
  )
  fmt.Printf("Find: %v\n", found)

  col.Update(Where {"Name": "Tucket" }, Set { "FooSet": "Bar", "Name": "Tucket3" })
  col.UpdateAll(Where {"Name": "Tucket5" }, Upsert { "Heh": "Bar" })
  col.Update(Where {"Name": "Tucket3" }, Modify { "$unset": "FooSet" })

  col.Remove(Where { "Name": "Tucket" })
  col.RemoveAll(Where { "Name": "Tucket" })
  */
}
