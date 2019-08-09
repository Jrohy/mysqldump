package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mysqldump/common"
	xlog "mysqldump/xlog"
)

// Files tuple.
type Files struct {
	schemas []string
	tables  []string
}

var (
	viewSuffix   = "-schema-view.sql"
	schemaSuffix = "-schema.sql"
	tableSuffix  = ".sql"
	dbName       = ""
)

func loadFiles(log *xlog.Log, dir string) *Files {
	var views []string
	files := &Files{}
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("loader.file.walk.error:%+v", err)
		}

		if !info.IsDir() {
			switch {
			case strings.HasSuffix(path, schemaSuffix):
				files.schemas = append(files.schemas, path)
			case strings.HasSuffix(path, viewSuffix):
				views = append(views, path)
			default:
				if strings.HasSuffix(path, tableSuffix) {
					files.tables = append(files.tables, path)
				}
			}
		}
		return nil
	}); err != nil {
		log.Panicf("loader.file.walk.error:%+v", err)
	}
	// put views schemas after table schemas
	if len(views) > 0 {
		files.schemas = append(files.schemas, views...)
	}
	return files
}

func restoreDatabaseSchema(log *xlog.Log, db string, conn *common.Connection) {
	if db == "" {
		metaFile := filepath.Base("dbname")
		data, err := common.ReadFile(metaFile)
		common.AssertNil(err)
		dbName = common.BytesToString(data)
	} else {
		dbName = db
	}
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", dbName)
	err := conn.Execute(sql)
	common.AssertNil(err)
	log.Info("restoring.database[%s]", dbName)
}

func restoreTableSchema(log *xlog.Log, overwrite bool, tables []string, conn *common.Connection) {
	for _, table := range tables {
		// use
		base := filepath.Base(table)
		name := strings.TrimSuffix(base, schemaSuffix)

		log.Info("working.table[%s]", name)

		err := conn.Execute(fmt.Sprintf("USE `%s`", dbName))
		common.AssertNil(err)

		err = conn.Execute("SET FOREIGN_KEY_CHECKS=0")
		common.AssertNil(err)

		data, err := common.ReadFile(table)
		common.AssertNil(err)
		query1 := common.BytesToString(data)
		querys := strings.Split(query1, ";\n")
		for _, query := range querys {
			if !strings.HasPrefix(query, "/*") && query != "" {
				if overwrite {
					log.Info("drop(overwrite.is.true).table[%s]", name)
					dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
					err = conn.Execute(dropQuery)
					common.AssertNil(err)
				}
				err = conn.Execute(query)
				common.AssertNil(err)
			}
		}
		log.Info("restoring.schema[%s]", name)
	}
}

func restoreTable(log *xlog.Log, table string, conn *common.Connection) int {
	bytes := 0
	part := "0"
	base := filepath.Base(table)
	name := strings.TrimSuffix(base, tableSuffix)
	splits := strings.Split(name, ".")
	tb := splits[0]
	if len(splits) > 1 {
		part = splits[1]
	}

	log.Info("restoring.tables[%s].parts[%s].thread[%d]", tb, part, conn.ID)
	err := conn.Execute(fmt.Sprintf("USE `%s`", dbName))
	common.AssertNil(err)

	err = conn.Execute("SET FOREIGN_KEY_CHECKS=0")
	common.AssertNil(err)

	data, err := common.ReadFile(table)
	common.AssertNil(err)
	query1 := common.BytesToString(data)
	querys := strings.Split(query1, ";\n")
	bytes = len(query1)
	for _, query := range querys {
		if !strings.HasPrefix(query, "/*") && query != "" {
			err = conn.Execute(query)
			common.AssertNil(err)
		}
	}
	log.Info("restoring.tables[%s].parts[%s].thread[%d].done...", tb, part, conn.ID)
	return bytes
}

// Loader used to start the loader worker.
func Loader(log *xlog.Log, args *common.Args) {
	pool, err := common.NewPool(log, args.Threads, args.Address, args.User, args.Password)
	common.AssertNil(err)
	defer pool.Close()

	files := loadFiles(log, args.Outdir)

	// database.
	conn := pool.Get()
	restoreDatabaseSchema(log, args.Database, conn)
	pool.Put(conn)

	// tables.
	conn = pool.Get()
	restoreTableSchema(log, args.OverwriteTables, files.schemas, conn)
	pool.Put(conn)

	// Shuffle the tables
	for i := range files.tables {
		j := rand.Intn(i + 1)
		files.tables[i], files.tables[j] = files.tables[j], files.tables[i]
	}

	var wg sync.WaitGroup
	var bytes uint64
	t := time.Now()
	for _, table := range files.tables {
		conn := pool.Get()
		wg.Add(1)
		go func(conn *common.Connection, table string) {
			defer func() {
				wg.Done()
				pool.Put(conn)
			}()
			r := restoreTable(log, table, conn)
			atomic.AddUint64(&bytes, uint64(r))
		}(conn, table)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			bytes := float64(atomic.LoadUint64(&bytes) / 1024 / 1024)
			rates := bytes / diff
			log.Info("restoring.allbytes[%vMB].time[%.2fsec].rates[%.2fMB/sec]...", bytes, diff, rates)
		}
	}()

	wg.Wait()
	elapsedStr, elapsed := time.Since(t).String(), time.Since(t).Seconds()
	log.Info("restoring.all.done.cost[%s].allbytes[%.2fMB].rate[%.2fMB/s]", elapsedStr, float64(bytes/1024/1024), float64(bytes/1024/1024)/elapsed)
}
