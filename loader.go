package main

import (
	"fmt"
	"github.com/go-xorm/xorm"
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
	procedures []string
	functions  []string
	tables     []string
	views      []string
	datas      []string
}

var (
	procedureSuffix = "-procedure.sql"
	functionSuffix  = "-function.sql"
	tableSuffix     = "-table.sql"
	viewSuffix      = "-view.sql"
	dataSuffix      = ".sql"
)

func loadFiles(log *xlog.Log, dir string) *Files {
	files := &Files{}
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("loader.file.walk.error:%+v", err)
		}

		if !info.IsDir() {
			switch {
			case strings.HasSuffix(path, tableSuffix):
				files.tables = append(files.tables, path)
			case strings.HasSuffix(path, functionSuffix):
				files.functions = append(files.functions, path)
			case strings.HasSuffix(path, procedureSuffix):
				files.procedures = append(files.procedures, path)
			case strings.HasSuffix(path, viewSuffix):
				files.views = append(files.views, path)
			default:
				if strings.HasSuffix(path, dataSuffix) {
					files.datas = append(files.datas, path)
				}
			}
		}
		return nil
	}); err != nil {
		log.Panicf("loader.file.walk.error:%+v", err)
	}
	return files
}

func restoreSchema(log *xlog.Log, engine *xorm.Engine, schemas []string, key string) {
	for _, schema := range schemas {
		name := strings.TrimSuffix(filepath.Base(schema), fmt.Sprintf("-%s.sql", key))
		dropQuery := fmt.Sprintf("DROP %s IF EXISTS %s", strings.ToUpper(key), name)
		_, err := engine.DB().Exec(dropQuery)
		common.AssertNil(err)

		data, err := common.ReadFile(schema)
		common.AssertNil(err)
		query := common.BytesToString(data)
		_, err = engine.DB().Exec(query)
		common.AssertNil(err)
		log.Info("restoring.schema.%s[%s]", key, name)
	}
}

func restoreData(log *xlog.Log, table string, engine *xorm.Engine) int {
	part := "0"
	base := filepath.Base(table)
	name := strings.TrimSuffix(base, dataSuffix)
	splits := strings.Split(name, ".")
	tb := splits[0]
	if len(splits) > 1 {
		part = splits[1]
	}

	log.Info("restoring.tables[%s].parts[%s]", tb, part)

	bytes, err := common.ReadFile(table)
	common.AssertNil(err)
	sqlStr := common.BytesToString(bytes)
	sqls := strings.Split(sqlStr, ";\n")
	for _, sql := range sqls {
		if sql != "" {
			_, _ = engine.DB().Exec("SET FOREIGN_KEY_CHECKS=0")
			_, err = engine.DB().Exec(sql)
			common.AssertNil(err)
		}
	}
	log.Info("restoring.tables[%s].parts[%s].done...", tb, part)
	return len(bytes)
}

// Loader used to start the loader worker.
func Loader(log *xlog.Log, args *common.Args, engine *xorm.Engine) {
	t := time.Now()
	files := loadFiles(log, args.Outdir)
	_, _ = engine.DB().Exec("SET FOREIGN_KEY_CHECKS=0")
	go restoreSchema(log, engine, files.functions, "function")
	go restoreSchema(log, engine, files.procedures, "procedure")
	restoreSchema(log, engine, files.tables, "table")
	restoreSchema(log, engine, files.views, "view")

	var wg sync.WaitGroup

	var bytes uint64
	for _, table := range files.datas {
		wg.Add(1)
		go func(engine *xorm.Engine, table string) {
			defer func() {
				wg.Done()
			}()
			r := restoreData(log, table, engine)
			atomic.AddUint64(&bytes, uint64(r))
		}(engine, table)
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
