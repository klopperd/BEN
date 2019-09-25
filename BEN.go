package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	server string
	port   = 1433
	user string
	password string
	database = "ben"
)

func pad(intext string, padding int) string {

	lentxt := len(intext)
	padlen := padding - lentxt

	for i := 1; i < padlen; i++ {
		intext = intext + " "
	}
	return intext
}

// ReadEmployees read all employees
func ReadUsers(db *sql.DB) (int, error) {
	tsql := fmt.Sprintf("SELECT Name, Username FROM dbo.aspnet_Users;")
	rows, err := db.Query(tsql)
	if err != nil {
		fmt.Println("Error reading rows: " + err.Error())
		return -1, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name, location string

		err := rows.Scan(&name, &location)
		if err != nil {
			fmt.Println("Error reading rows: " + err.Error())
			return -1, err
		}
		fmt.Printf("%s \t %s\n", pad(name, 25), location)
		count++
	}
	return count, nil
}

func GetLatest(db *sql.DB) (int, error) {
	tsql := fmt.Sprintf("SELECT top 2 ScadaSystemDate,ReceiveTime,Message," +
		"DATEDIFF(SECOND ,ScadaSystemDate, ReceiveTime) as DT FROM BEN.dbo.Messages" +
		" order by ReceiveTime desc;")
	rows, err := db.Query(tsql)
	if err != nil {
		fmt.Println("Error reading rows: " + err.Error())
		return -1, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ScadaSystemDate, ReceiveTime, Message, DT string

		err := rows.Scan(&ScadaSystemDate, &ReceiveTime, &Message, &DT)
		if err != nil {
			fmt.Println("Error reading rows: " + err.Error())
			return -1, err
		}
		fmt.Printf("%s \t %s \t %s \n", ReceiveTime, Message, DT)
		count++
	}
	return count, nil
}

func NotAudit(db *sql.DB) (int, error) {
	tsql := fmt.Sprintf("SELECT top 10 Timestamp, Message," +
		"destination, messageid FROM BEN.dbo.NotificationAudit" +
		" order by Timestamp desc;")
	rows, err := db.Query(tsql)
	if err != nil {
		fmt.Println("Error reading rows: " + err.Error())
		return -1, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var TS, Mess, dest, id string

		err := rows.Scan(&TS, &Mess, &dest, &id)
		if err != nil {
			fmt.Println("Error reading rows: " + err.Error())
			return -1, err
		}
		fmt.Printf("%s \t %s \t %s \t %s \n", TS, Mess, dest, id)
		count++
	}
	return count, nil
}

func Summary(db *sql.DB) (int, error) {
	tsql := fmt.Sprintf("SELECT top 1 ScadaSystemDate,ReceiveTime,Message," +
		"DATEDIFF(SECOND ,ScadaSystemDate, ReceiveTime) as DT FROM BEN.dbo.Messages" +
		" order by ReceiveTime desc;")
	rows, err := db.Query(tsql)
	if err != nil {
		fmt.Println("Error reading rows: " + err.Error())
		return -1, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ScadaSystemDate, ReceiveTime, Message, DT string

		err := rows.Scan(&ScadaSystemDate, &ReceiveTime, &Message, &DT)
		if err != nil {
			fmt.Println("Error reading rows: " + err.Error())
			return -1, err
		}
		fmt.Println("Last Message received   : " + ReceiveTime)
		fmt.Println("Last Message Scada Time : " + ScadaSystemDate)
		fmt.Println("Last Message            : " + Message)
		fmt.Println("Last Message DT         : " + DT)

		//fmt.Printf("%s \t %s \t %s \n", ReceiveTime, Message, DT)
		count++
	}
	return count, nil
}

func main() {

	// Connect to database
	//connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
	//	server, user, password, port, database)

	if len(os.Args) > 1 {
		args := os.Args
		//fmt.Println(args[1])
		user = "elec\\klopperd"
		password = "detany1909"
		switch args[1] {
		case "wc":
			fmt.Println("-----------WCOU---------------")
			server = "blvvmsa014\\scadaapps"
		case "ec":
			fmt.Println("-----------ECOU---------------")
			server = "elnvmsa006\\scadaapps"
		case "kz":
			fmt.Println("-----------KZOU---------------")
			server = "mkdvmsa006\\scadaapps"
		case "mp":
			fmt.Println("-----------MPOU---------------")
			server = "wtkvmsa008\\scadaapps"
		case "gt":
			fmt.Println("-----------GTOU---------------")
			server = "spnvmsa010\\scadaapps"
		default:
			fmt.Println("-----------TEST---------------")
			server = "hwhvmsa004"
			user = "qaelec\\klopperd"
			password = "Eskom899#"
		}
	} else {
		// No args passed
		fmt.Println("-----------TEST---------------")
		server = "hwhvmsa004"
		user = "qaelec\\klopperd"
		password = "Eskom899#"
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;",
		server, user, password, database)

	conn, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	fmt.Printf("Connected!\n")
	//fmt.Println(reg)
	defer conn.Close()

	// Read employees
	//count, err := ReadUsers(conn)
	//if err != nil {
	//	log.Fatal("ReadUsers failed:", err.Error())
	//}
	//fmt.Printf("Read %d rows successfully.\n", count)

	// Read employees
	//mcount, merr := GetLatest(conn)
	//if err != nil {
	//	log.Fatal("ReadUsers failed:", merr.Error())
	//}
	//fmt.Printf("Read %d rows successfully.\n", mcount)

	// Get Summary
	mcount, merr := Summary(conn)
	if err != nil {
		log.Fatal("Summary failed:", merr.Error())
	}
	fmt.Printf("Read %d rows successfully.\n", mcount)

	//conn.Close()

	ncount, nerr := NotAudit(conn)
	if err != nil {
		log.Fatal("Summary failed:", nerr.Error())
	}
	fmt.Printf("Read %d rows successfully.\n", ncount)

	conn.Close()
}
