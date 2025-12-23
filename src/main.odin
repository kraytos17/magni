package main

import "src:db"

main :: proc() {
	database, ok := db.db_open("test.db")
	if !ok {
		return
	}

	defer db.db_close(database)
	db.db_execute(
		database,
		`
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER
        )
    `,
	)

	db.db_execute(database, "INSERT INTO users VALUES (1, 'Alice', 30)")
	db.db_execute(database, "INSERT INTO users VALUES (2, 'Bob', 25)")
	db.db_execute(database, "SELECT * FROM users")

	db.db_debug_dump_table(database, "users")
}
