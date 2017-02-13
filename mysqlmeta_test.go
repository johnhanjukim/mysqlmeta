package mysqlmeta_test
import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestGetColumns(t *testing.T) {
	src := "root:local@tcp(localhost:3306)/daps?charset=utf8"
	db, err := sql.Open("mysql", src)
	cols, err := mysqlmeta.GetColumns(db, "user_account")
	fmt.Printf("got %d columns\n", len(cols))
}
