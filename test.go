package main
import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
	"mysqlmeta"
	"os"
	"time"
)

func logTime(start int64, step string) {
	ms := float64((time.Now().UnixNano()-start)/1000) / 1000.0
	fmt.Printf("%.3f (%s)\n", ms, step)
}
func checkErr(err error) {
	if nil != err {
		panic(err)
	}
}
func main() {
	start := time.Now().UnixNano()
	src := "root:local@tcp(localhost:3306)/daps?charset=utf8"
	if (len(os.Args) > 1) && ("prod" == os.Args[1]) {
		src = "kidaptiveshop:kidaptive2014@tcp(hodooshopstaging.ca1jql6qyqdz.us-west-1.rds.amazonaws.com:3306)/daps?charset=utf8"
	}
	db, err := sql.Open("mysql", src)

	cols, err := mysqlmeta.GetColumns(db, "user_account")
	fmt.Printf("got %d columns\n", len(cols))
	
	logTime(start, "open sql")
	checkErr(err)
	// query
	start = time.Now().UnixNano()
	rows, err := db.Query("SELECT unix_timestamp()")
	logTime(start, "first query sql")
	checkErr(err)
	// read rows
	start = time.Now().UnixNano()
	for rows.Next() {
		var num int
		err = rows.Scan(&num)
		checkErr(err)
	}
	logTime(start, "first read rows")
	// second query
	start = time.Now().UnixNano()
	rows, err = db.Query("SELECT id, organization_id, email from user_account where id = 1")
	logTime(start, "second query")
	checkErr(err)
	// second read
	start = time.Now().UnixNano()
	for rows.Next() {
		var id int
		var gn string
		var fn string
		err = rows.Scan(&id, &gn, &fn)
		checkErr(err)
	}
	logTime(start, "second read")
	db.Close()
}
