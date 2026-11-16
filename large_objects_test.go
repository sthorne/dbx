package pgx_test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestLargeObjects(t *testing.T) {
	// We use a very short limit to test chunking logic.
	pgx.SetMaxLargeObjectMessageLength(t, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}

	pgxtest.SkipCockroachDB(t, conn, "Server does support large objects")

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	testLargeObjects(t, ctx, tx)
}

func TestLargeObjectsSimpleProtocol(t *testing.T) {
	// We use a very short limit to test chunking logic.
	pgx.SetMaxLargeObjectMessageLength(t, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}

	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	pgxtest.SkipCockroachDB(t, conn, "Server does support large objects")

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	testLargeObjects(t, ctx, tx)
}

func testLargeObjects(t *testing.T, ctx context.Context, tx pgx.Tx) {
	lo := tx.LargeObjects()

	id, err := lo.Create(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	obj, err := lo.Open(ctx, id, pgx.LargeObjectModeRead|pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}

	n, err := obj.Write([]byte("testing"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Errorf("Expected n to be 7, got %d", n)
	}

	pos, err := obj.Seek(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 1 {
		t.Errorf("Expected pos to be 1, got %d", pos)
	}

	res := make([]byte, 6)
	n, err = obj.Read(res)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "esting" {
		t.Errorf(`Expected res to be "esting", got %q`, res)
	}
	if n != 6 {
		t.Errorf("Expected n to be 6, got %d", n)
	}

	n, err = obj.Read(res)
	if err != io.EOF {
		t.Error("Expected io.EOF, go nil")
	}
	if n != 0 {
		t.Errorf("Expected n to be 0, got %d", n)
	}

	pos, err = obj.Tell()
	if err != nil {
		t.Fatal(err)
	}
	if pos != 7 {
		t.Errorf("Expected pos to be 7, got %d", pos)
	}

	err = obj.Truncate(1)
	if err != nil {
		t.Fatal(err)
	}

	pos, err = obj.Seek(-1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("Expected pos to be 0, got %d", pos)
	}

	res = make([]byte, 2)
	n, err = obj.Read(res)
	if err != io.EOF {
		t.Errorf("Expected err to be io.EOF, got %v", err)
	}
	if n != 1 {
		t.Errorf("Expected n to be 1, got %d", n)
	}
	if res[0] != 't' {
		t.Errorf("Expected res[0] to be 't', got %v", res[0])
	}

	err = obj.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = lo.Unlink(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	_, err = lo.Open(ctx, id, pgx.LargeObjectModeRead)
	if e, ok := err.(*pgconn.PgError); !ok || e.Code != "42704" {
		t.Errorf("Expected undefined_object error (42704), got %#v", err)
	}
}

func TestLargeObjectsMultipleTransactions(t *testing.T) {
	// We use a very short limit to test chunking logic.
	pgx.SetMaxLargeObjectMessageLength(t, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}

	pgxtest.SkipCockroachDB(t, conn, "Server does support large objects")

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	lo := tx.LargeObjects()

	id, err := lo.Create(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	obj, err := lo.Open(ctx, id, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}

	n, err := obj.Write([]byte("testing"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Errorf("Expected n to be 7, got %d", n)
	}

	// Commit the first transaction
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// IMPORTANT: Use the same connection for another query
	query := `select n from generate_series(1,10) n`
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	// Start a new transaction
	tx2, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	lo2 := tx2.LargeObjects()

	// Reopen the large object in the new transaction
	obj2, err := lo2.Open(ctx, id, pgx.LargeObjectModeRead|pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}

	pos, err := obj2.Seek(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 1 {
		t.Errorf("Expected pos to be 1, got %d", pos)
	}

	res := make([]byte, 6)
	n, err = obj2.Read(res)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "esting" {
		t.Errorf(`Expected res to be "esting", got %q`, res)
	}
	if n != 6 {
		t.Errorf("Expected n to be 6, got %d", n)
	}

	n, err = obj2.Read(res)
	if err != io.EOF {
		t.Error("Expected io.EOF, go nil")
	}
	if n != 0 {
		t.Errorf("Expected n to be 0, got %d", n)
	}

	pos, err = obj2.Tell()
	if err != nil {
		t.Fatal(err)
	}
	if pos != 7 {
		t.Errorf("Expected pos to be 7, got %d", pos)
	}

	err = obj2.Truncate(1)
	if err != nil {
		t.Fatal(err)
	}

	pos, err = obj2.Seek(-1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("Expected pos to be 0, got %d", pos)
	}

	res = make([]byte, 2)
	n, err = obj2.Read(res)
	if err != io.EOF {
		t.Errorf("Expected err to be io.EOF, got %v", err)
	}
	if n != 1 {
		t.Errorf("Expected n to be 1, got %d", n)
	}
	if res[0] != 't' {
		t.Errorf("Expected res[0] to be 't', got %v", res[0])
	}

	err = obj2.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = lo2.Unlink(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	_, err = lo2.Open(ctx, id, pgx.LargeObjectModeRead)
	if e, ok := err.(*pgconn.PgError); !ok || e.Code != "42704" {
		t.Errorf("Expected undefined_object error (42704), got %#v", err)
	}
}

// A failed loread must not be counted as bytes read. Read used to add the length of
// its preallocated buffer before checking the error, so it returned a full count
// together with the error, and io.ReadFull, which keeps an error only when the count
// falls short, silently dropped it.
func TestLargeObjectsReadErrorReportsNoBytes(t *testing.T) {
	// Several chunks, so the failure is on a later message, after some bytes were read.
	pgx.SetMaxLargeObjectMessageLength(t, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	pgxtest.SkipCockroachDB(t, conn, "Server does support large objects")

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	lo := tx.LargeObjects()
	id, err := lo.Create(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := lo.Open(ctx, id, pgx.LargeObjectModeRead|pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Write([]byte("testing")); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	// Closing the descriptor makes every later loread fail.
	if err := obj.Close(); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 7)
	n, err := obj.Read(buf)
	if err == nil {
		t.Fatal("Read on a closed descriptor returned no error")
	}
	if n != 0 {
		t.Errorf("Read returned %d bytes along with %v; want 0", n, err)
	}

	// The case that hid the error: io.ReadFull over the object. The failed loread
	// aborted the transaction, so this read fails too; the error must reach the caller.
	n, err = io.ReadFull(obj, buf)
	if err == nil {
		t.Fatalf("io.ReadFull returned %d bytes and no error after a failed loread", n)
	}
}
