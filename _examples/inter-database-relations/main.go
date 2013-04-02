package main

/*
	See relations: http://gosexy.org/db/collection
*/

import (
	"fmt"
	"menteslibres.net/gosexy/db"
	_ "menteslibres.net/gosexy/db/mysql"
	_ "menteslibres.net/gosexy/db/postgresql"
)

/*
	create table "peopleIAdmire" (
		id serial,
		name varchar,
		born int
	);
*/
var postgresqlSettings = db.DataSource{
	//Host:     "localhost",
	Socket:   "/var/run/postgresql/",
	Database: "myself",
	User:     "gouser",
	Password: "gopass",
}

/*
	create table worksOfPeopleIAdmire (
		id smallint auto_increment,
		primary key(id),
		name varchar(255),
		year smallint,
		author_id varchar(60)
	);
*/
var mysqlSettings = db.DataSource{
	//Host:     "localhost",
	Socket:   "/var/run/mysqld/mysqld.sock",
	Database: "myself",
	User:     "gouser",
	Password: "gopass",
}

func main() {

	var ids []db.Id
	var err error

	mysqlSess, err := db.Open("mysql", mysqlSettings)

	if err != nil {
		panic(err)
	}

	defer mysqlSess.Close()

	postgresqlSess, err := db.Open("postgresql", postgresqlSettings)

	if err != nil {
		panic(err)
	}

	defer postgresqlSess.Close()

	peopleIAdmire := postgresqlSess.ExistentCollection("peopleIAdmire")
	worksOfPeopleIAdmire := mysqlSess.ExistentCollection("worksOfPeopleIAdmire")

	// APPENDING PEOPLE
	peopleIAdmire.Truncate()

	// Hayao Miyazaki
	ids, err = peopleIAdmire.Append(db.Item{
		"name": "Hayao Miyazaki",
		"born": 1941,
	})

	if err != nil {
		panic(err)
	}

	miyazakiId := ids[0]

	// Edgar Allan Poe
	ids, err = peopleIAdmire.Append(db.Item{
		"name": "Edgar Allan Poe",
		"born": 1809,
	})

	if err != nil {
		panic(err)
	}

	poeId := ids[0]

	// Gabriel García Márquez
	ids, err = peopleIAdmire.Append(db.Item{
		"name": "Gabriel García Márquez",
		"born": 1927,
	})

	if err != nil {
		panic(err)
	}

	gaboId := ids[0]

	// APPENDING WORKS
	worksOfPeopleIAdmire.Truncate()

	// Mizayaki
	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "Nausicaä of the Valley of the Wind",
		"year":      1984,
		"author_id": miyazakiId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "Princes Mononoke",
		"year":      1997,
		"author_id": miyazakiId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "Howl's Moving Castle",
		"year":      2004,
		"author_id": miyazakiId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "My Neighbor Totoro",
		"year":      1988,
		"author_id": miyazakiId,
	})

	// Poe
	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "The Black Cat",
		"year":      1843,
		"author_id": poeId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "The Facts in the Case of M. Valdemar",
		"year":      1845,
		"author_id": poeId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "The Gold Bug",
		"year":      1843,
		"author_id": poeId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "The Murders in the Rue Morge",
		"year":      1841,
		"author_id": poeId,
	})

	// Gabo
	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "Memoria de mis putas tristes",
		"year":      2004,
		"author_id": gaboId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "El amor en los tiempos del cólera",
		"year":      1985,
		"author_id": gaboId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "Del amor y otros demonios",
		"year":      1994,
		"author_id": gaboId,
	})

	worksOfPeopleIAdmire.Append(db.Item{
		"name":      "Cien años de soledad",
		"year":      1967,
		"author_id": gaboId,
	})

	// TESTING RELATION

	peopleAndWorks, err := peopleIAdmire.FindAll(
		db.RelateAll{
			"works": db.On{
				worksOfPeopleIAdmire,
				db.Cond{"author_id": "{id}"},
			},
		},
	)

	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("People I Admire:\n\n")

	for _, person := range peopleAndWorks {

		fmt.Printf("%s. Born %d.\n\n", person["name"], person["born"])
		fmt.Printf("Some of his works are:\n")

		for _, work := range person["works"].([]db.Item) {
			fmt.Printf("* %s, %d.\n", work["name"], work["year"])
		}

		fmt.Printf("---\n\n")
	}

	/*
		People I Admire:

		Hayao Miyazaki. Born 1941.

		Some of his works are:
		* Nausicaä of the Valley of the Wind, 1984.
		* Princes Mononoke, 1997.
		* Howl's Moving Castle, 2004.
		* My Neighbor Totoro, 1988.
		---

		Edgar Allan Poe. Born 1809.

		Some of his works are:
		* The Black Cat, 1843.
		* The Facts in the Case of M. Valdemar, 1845.
		* The Gold Bug, 1843.
		* The Murders in the Rue Morge, 1841.
		---

		Gabriel García Márquez. Born 1927.

		Some of his works are:
		* Memoria de mis putas tristes, 2004.
		* El amor en los tiempos del cólera, 1985.
		* Del amor y otros demonios, 1994.
		* Cien años de soledad, 1967.
		---
	*/

}
