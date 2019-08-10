package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"mysqldump/common"
	xlog "mysqldump/xlog"
)

var excludeTable string

func writeDBName(args *common.Args) {
	file := fmt.Sprintf("%s/dbname", args.Outdir)
	_ = common.WriteFile(file, args.Database)
}

func dumpFunctionSchema(log *xlog.Log, conn *common.Connection, args *common.Args) {
	qr, err := conn.Fetch(fmt.Sprintf("SELECT `name` FROM mysql.proc WHERE type = 'FUNCTION' AND db = '%s'", args.Database))
	common.AssertNil(err)

	for _, t := range qr.Rows {
		function := t[0].String()
		qr, err := conn.Fetch(fmt.Sprintf("SHOW CREATE FUNCTION `%s`.`%s`", args.Database, function))
		common.AssertNil(err)

		schema := qr.Rows[0][2].String() + ";\n"
		file := fmt.Sprintf("%s/%s-schema-function.sql", args.Outdir, function)
		_ = common.WriteFile(file, schema)
		log.Info("dumping.function[%s.%s].schema...", args.Database, function)
	}
}

func dumpTableSchema(log *xlog.Log, conn *common.Connection, args *common.Args, table string) {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", args.Database, table))
	common.AssertNil(err)
	schema := qr.Rows[0][1].String() + ";\n"

	var file string
	if strings.Contains(schema, "DEFINER VIEW") {
		file = fmt.Sprintf("%s/%s-schema-view.sql", args.Outdir, table)
		//exclude view data
		excludeTable = excludeTable + table + ","
	} else {
		file = fmt.Sprintf("%s/%s-schema.sql", args.Outdir, table)
	}
	_ = common.WriteFile(file, schema)
	log.Info("dumping.table[%s.%s].schema...", args.Database, table)
}

func dumpTable(log *xlog.Log, conn *common.Connection, args *common.Args, table string) {
	var allBytes uint64
	var allRows uint64

	cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT /*backup*/ * FROM `%s`.`%s`", args.Database, table))
	common.AssertNil(err)

	fields := make([]string, 0, 16)
	flds := cursor.Fields()
	for _, fld := range flds {
		fields = append(fields, fmt.Sprintf("`%s`", fld.Name))
	}

	fileNo := 1
	stmtsize := 0
	chunkbytes := 0
	rows := make([]string, 0, 256)
	inserts := make([]string, 0, 256)
	for cursor.Next() {
		row, err := cursor.RowValues()
		common.AssertNil(err)

		values := make([]string, 0, 16)
		for _, v := range row {
			if v.Raw() == nil {
				values = append(values, "NULL")
			} else {
				str := v.String()
				switch {
				case v.IsSigned(), v.IsUnsigned(), v.IsFloat(), v.IsIntegral(), v.Type() == querypb.Type_DECIMAL:
					values = append(values, str)
				default:
					values = append(values, fmt.Sprintf("\"%s\"", common.EscapeBytes(v.Raw())))
				}
			}
		}
		r := "(" + strings.Join(values, ",") + ")"
		rows = append(rows, r)

		allRows++
		stmtsize += len(r)
		chunkbytes += len(r)
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if stmtsize >= args.StmtSize {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
			rows = rows[:0]
			stmtsize = 0
		}

		if (chunkbytes / 1024 / 1024) >= args.ChunksizeInMB {
			query := strings.Join(inserts, ";\n") + ";\n"
			file := fmt.Sprintf("%s/%s.%05d.sql", args.Outdir, table, fileNo)
			_ = common.WriteFile(file, query)

			log.Info("dumping.table[%s.%s].rows[%v].bytes[%vMB].part[%v].thread[%d]", args.Database, table, allRows, allBytes/1024/1024, fileNo, conn.ID)
			inserts = inserts[:0]
			chunkbytes = 0
			fileNo++
		}
	}
	if chunkbytes > 0 {
		if len(rows) > 0 {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
		}

		query := strings.Join(inserts, ";\n") + ";\n"
		file := fmt.Sprintf("%s/%s.%05d.sql", args.Outdir, table, fileNo)
		_ = common.WriteFile(file, query)
	}
	err = cursor.Close()
	common.AssertNil(err)

	log.Info("dumping.table[%s.%s].done.allrows[%v].allbytes[%vMB].thread[%d]...", args.Database, table, allRows, allBytes/1024/1024, conn.ID)
}

func allTables(conn *common.Connection, args *common.Args) []string {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW TABLES FROM `%s`", args.Database))
	common.AssertNil(err)

	tables := make([]string, 0, 128)
	for _, t := range qr.Rows {
		tables = append(tables, t[0].String())
	}
	return tables
}

// Dumper used to start the dumper worker.
func Dumper(log *xlog.Log, args *common.Args) {
	pool, err := common.NewPool(log, args.Threads, args.Address, args.User, args.Password)
	common.AssertNil(err)
	defer pool.Close()

	// database name
	writeDBName(args)

	// database.
	conn := pool.Get()

	var wg sync.WaitGroup
	var tables []string
	t := time.Now()

	//function
	dumpFunctionSchema(log, conn, args)

	//table
	if args.ExcludeTables != "" {
		excludeTable = excludeTable + args.ExcludeTables + ","
	}
	if args.Table != "" {
		tables = strings.Split(args.Table, ",")
	} else {
		tables = allTables(conn, args)
	}
	pool.Put(conn)

	for _, table := range tables {
		conn := pool.Get()
		dumpTableSchema(log, conn, args, table)
		wg.Add(1)
		go func(conn *common.Connection, table string) {
			defer func() {
				wg.Done()
				pool.Put(conn)
			}()
			// excludeTable can't dump data
			if !strings.Contains(excludeTable, table) {
				log.Info("dumping.table[%s.%s].datas.thread[%d]...", args.Database, table, conn.ID)
				dumpTable(log, conn, args, table)
				log.Info("dumping.table[%s.%s].datas.thread[%d].done...", args.Database, table, conn.ID)
			}
		}(conn, table)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			allbytesMB := float64(atomic.LoadUint64(&args.Allbytes) / 1024 / 1024)
			allrows := atomic.LoadUint64(&args.Allrows)
			rates := allbytesMB / diff
			log.Info("dumping.allbytes[%vMB].allrows[%v].time[%.2fsec].rates[%.2fMB/sec]...", allbytesMB, allrows, diff, rates)
		}
	}()

	wg.Wait()
	elapsedStr, elapsed := time.Since(t).String(), time.Since(t).Seconds()
	log.Info("dumping.all.done.cost[%s].allrows[%v].allbytes[%v].rate[%.2fMB/s]", elapsedStr, args.Allrows, args.Allbytes, float64(args.Allbytes/1024/1024)/elapsed)
}
