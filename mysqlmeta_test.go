package mysqlmeta

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"testing"
)

var (
	user   string
	pass   string
	prot   string
	addr   string
	dbname string
	dsn    string
)

// Use environment variables to define the testing database.
// Default is root@tcp(localhost:3306)/gotest
// Testing standards are the same as for mysql driver.
// See https://github.com/go-sql-driver/mysql/wiki/Testing
func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user = env("MYSQL_TEST_USER", "root")
	pass = env("MYSQL_TEST_PASS", "")
	prot = env("MYSQL_TEST_PROT", "tcp")
	addr = env("MYSQL_TEST_ADDR", "localhost:3306")
	dbname = env("MYSQL_TEST_DBNAME", "gotest")
	dsn = fmt.Sprintf("%s:%s@%s(%s)/%s?timeout=30s&strict=true", user, pass, prot, addr, dbname)
}

func mustGetDB(t *testing.T) sql.DB {
	db, err := sql.Open("mysql", dsn)
	if nil != err {
		t.Fatalf("error getting db connection\n%v", err)
	}
	defer db.Close()
	return *db
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) sql.Result {
	result, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("failed to exec\n%s\n%v", query, err)
	}
	return result
}

func TestGetColumns(t *testing.T) {
	db := mustGetDB(t)
	db.Exec("DROP TABLE IF EXISTS test")
	mustExec(t, &db, "CREATE TABLE test (id INT, value BOOL, name VARCHAR(255))")
	cols, err := GetColumns(&db, "test")
	if nil != err || (len(cols) != 3) {
		t.Fatalf("columns not found")
	}
	_, err = GetIndexes(&db, "test", cols)
	if nil != err {
		t.Fatalf("error getting indexes\n%v", err)
	}
	e := struct {
		Id    int
		Value bool
		Name  string
	}{}
	_, err = GetTableMetadata(&db, "test", &e)
	if nil != err {
		t.Fatalf("error getting metadata\n%v", err)
	}
}
