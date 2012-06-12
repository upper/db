package db

import (
  . "github.com/xiam/gosexy"
  "testing"
  "fmt"
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

  // Testing insert
  col.Append(Tuple { "Name": "Tucket11", "LastName": "Nancy" })
  
  /*
  col.Find()
  
  col.Find(
    Where { "Name": "Tucket" },
  )
  
  col.Find(
    Where { "Name": "Tucket" },
    Where { "LastName $ne": "Barr" },
    Limit (2),
    Offset (5),
    Sort { "Name": -1 },
  )
  */
  
  found := col.Find(
    Where { "Name": "Tucket3" },
    Where { "LastName $ne": "Barr" },
  )
  fmt.Printf("Find: %v\n", found)

  col.Update(Where {"Name": "Tucket" }, Set { "FooSet": "Bar", "Name": "Tucket3" })
  col.Update(Where {"Name": "Tucket5" }, Upsert { "Heh": "Bar" })
  col.Update(Where {"Name": "Tucket3" }, Modify { "$unset": "FooSet" })

  col.Remove(Where { "Name": "Tucket" })
  col.RemoveAll(Where { "Name": "Tucket" })
}
