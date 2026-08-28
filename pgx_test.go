package dbx_test

import (
	"context"
	"os"
	"testing"

	"github.com/sthorne/dbx/v5"
	_ "github.com/sthorne/dbx/v5/stdlib"
)

func skipCockroachDB(t testing.TB, msg string) {
	conn, err := dbx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip(msg)
	}
}
