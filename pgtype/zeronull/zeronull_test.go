package zeronull_test

import (
	"context"
	"os"
	"testing"

	"github.com/sthorne/dbx/v5"
	"github.com/sthorne/dbx/v5/pgtype/zeronull"
	"github.com/sthorne/dbx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner pgxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = pgxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *dbx.ConnConfig {
		config, err := dbx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		return config
	}
	defaultConnTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *dbx.Conn) {
		zeronull.Register(conn.TypeMap())
	}
}
