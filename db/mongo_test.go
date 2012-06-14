package db

import (
  //. "github.com/xiam/gosexy"
  "fmt"
  "math/rand"
  "testing"
)

/*
func TestConnect(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "dns.fail", Database: "test" })

  err := db.Connect()

  if err != nil {
    t.Logf("Got %t, this was intended.", err)
    return
  }

  t.Error("Are you serious?")
}

func TestAuthFail(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test", User: "unknown", Password: "fail" })

  err := db.Connect()

  if err != nil {
    t.Logf("Got %t, this was intended.", err)
    return
  }

  t.Error("Are you serious?")
}
*/

func TestDrop(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  db.Drop()
}

func TestAppend(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  col := db.Collection("people")

  names := []string { "Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel" }

  for i := 0; i < len(names); i++ {
    col.Append(Item { "name": names[i] })
  }

  if col.Count() != len(names) {
    t.Error("Could not append all items.")
  }


}

func TestFind(t *testing.T) {

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  col := db.Collection("people")

  result := col.Find(Where { "name": "José" })

  if result["name"] != "José" {
    t.Error("Could not find a recently appended item.")
  }

}

func TestDelete(t *testing.T) {
  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  col := db.Collection("people")

  col.Remove(Where { "name": "Juan" })
  
  result := col.Find(Where { "name": "Juan" })

  if len(result) > 0 {
    t.Error("Could not remove a recently appended item.")
  }
}

func TestUpdate(t *testing.T) {
  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  col := db.Collection("people")

  col.Update(Where { "name": "José" }, Set { "name": "Joseph"})
  
  result := col.Find(Where { "name": "Joseph" })

  if len(result) == 0 {
    t.Error("Could not update a recently appended item.")
  }
}

func TestPopulate(t *testing.T) {
  var i int

  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  states := []string { "Alaska", "Alabama", "Cancún" }

  for i = 0; i < len(states); i++ {
    db.Collection("states").Append(Item {
      "code_id": i,
      "name": states[i],
    })
  }

  people := db.Collection("people").FindAll()

  for i = 0; i < len(people); i++ {
    person := people[i]

    // Has 5 children.
    for j := 0; j < 5; j++ {
      db.Collection("children").Append(Item {
        "name": fmt.Sprintf("%s's child %d", person["name"], j + 1),
        "parent_id": person["_id"],
      })
    }

    // Belongs to one State.
    db.Collection("people").Update(
      Where { "_id": person["_id"] },
      Set { "state_code_id": int( rand.Float32() * float32(len(states)) ) },
    )
  }

}

func TestRelation(t *testing.T) {
  db := NewMongoDB(&DataSource{ Host: "localhost", Database: "test" })

  err := db.Connect()

  if err != nil {
    panic(err)
  }

  db.Use("test")

  col := db.Collection("people")

  col.FindAll(
    Relate {
      "state": On {
        db.Collection("states"),
        Where { "{this}.code_id": "{that}.state_code_id" },
      },
      "children": On {
        Where { "{this}.parent_id": "{that}._id" },
        Limit(4),
      },
    },
  )
}

