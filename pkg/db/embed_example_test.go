package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/unhandled-exception/sophiadb/pkg/db"
)

func Example() {
	dir, err := os.MkdirTemp("", "sophia_data")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(dir)

	db, err := sql.Open("sophiadb:embed", dir)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	ctx := context.Background()

	con, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer con.Close()

	_, err = con.ExecContext(ctx, "create table table1 (id int64, name varchar(100))")
	if err != nil {
		log.Fatal("create: ", err)
	}

	tx, err := con.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		_, err = tx.ExecContext(ctx, "insert into table1 (id, name) values (?, ?)", i, fmt.Sprintf("name '%d'", i))
		if err != nil {
			log.Fatal("insert: ", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	user := struct {
		id   int64
		name string
	}{}

	err = con.QueryRowContext(ctx, "select id, name from table1 where id = :id", sql.Named("id", 2)).Scan(&user.id, &user.name)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", user)

	// Output: {id:2 name:name '2'}
}
