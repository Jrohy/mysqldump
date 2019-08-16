package main

import (
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"mysqldump/common"
	xlog "mysqldump/xlog"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const Pattern = `\w+:\w+@[\w.]+:\d{0,5}$`

var (
	engine                                                                                            *xorm.Engine
	flagChunksize, flagThreads, flagPort, flagStmtSize                                                int
	flagUser, flagPasswd, flagHost, flagSource, flagDb, flagOutputDir, flagInputDir, flagExcludeTable string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the dump")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDb, "db", "", "Database to dump or database to import")
	flag.StringVar(&flagOutputDir, "o", "", "Directory to output files to")
	flag.StringVar(&flagInputDir, "i", "", "Directory of the dump to import")
	flag.IntVar(&flagChunksize, "F", 128, "Split tables into chunks of this output file size. This value is in MB")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.IntVar(&flagStmtSize, "s", 1000000, "Attempted size of INSERT statement in bytes")
	flag.StringVar(&flagSource, "m", "", "Mysql source info in one string, format: user:password@host:port")
	flag.StringVar(&flagExcludeTable, "exclude", "", "Do not dump the specified table data, use ',' to split multiple table")
	flag.Usage = usage
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR] -i [INDIR] -m [MYSQL_SOURCE] -exclude [EXCLUDE_TABLE]")
	flag.PrintDefaults()
	os.Exit(0)
}

func splitSource(input string) (string, string, string, int) {
	sourceSlice := strings.Split(input, "@")
	userSlice := strings.Split(sourceSlice[0], ":")
	addressSlice := strings.Split(sourceSlice[1], ":")
	port, _ := strconv.Atoi(addressSlice[1])
	return userSlice[0], userSlice[1], addressSlice[0], port
}

func createDatabase(path string) {
	if flagDb == "" {
		data, err := common.ReadFile(path + "/dbname")
		common.AssertNil(err)
		flagDb = common.BytesToString(data)
	}
	engine, _ = xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		flagUser, flagPasswd, flagHost, flagPort))
	_, err := engine.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", flagDb))
	common.AssertNil(err)
	log.Info("restoring.database[%s]", flagDb)
}

func generateArgs() *common.Args {
	var flagDir string

	if flagSource != "" {
		if check, _ := regexp.Match(Pattern, []byte(flagSource)); check {
			flagUser, flagPasswd, flagHost, flagPort = splitSource(flagSource)
		} else {
			fmt.Printf("%s can't match regex 'user:password@host:port'", flagSource)
			os.Exit(0)
		}
	}

	if flagHost == "" || flagUser == "" {
		usage()
		os.Exit(0)
	}

	if flagInputDir != "" && flagOutputDir != "" {
		fmt.Println("can't use '-i' and '-o' flag at the same time!")
		os.Exit(0)
	} else if flagInputDir == "" && flagOutputDir == "" {
		fmt.Println("must have flag '-i' or '-o'!")
		os.Exit(0)
	}

	if flagOutputDir != "" {
		if flagDb == "" {
			fmt.Println("must have flag '-db' to special database to dump ")
			os.Exit(0)
		}
		if _, err := os.Stat(flagOutputDir); os.IsNotExist(err) {
			x := os.MkdirAll(flagOutputDir, 0777)
			common.AssertNil(x)
		}
		flagDir = flagOutputDir
	} else {
		flagDir = flagInputDir
		createDatabase(flagDir)
	}

	args := &common.Args{
		Database:      flagDb,
		Outdir:        flagDir,
		ChunksizeInMB: flagChunksize,
		Threads:       flagThreads,
		StmtSize:      flagStmtSize,
		IntervalMs:    10 * 1000,
		ExcludeTables: flagExcludeTable,
	}

	return args
}

func main() {
	flag.Parse()
	args := generateArgs()
	engine, _ = xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8",
		flagUser, flagPasswd, flagHost, flagPort, flagDb))
	if flagOutputDir != "" {
		Dumper(log, args, engine)
	} else {
		Loader(log, args, engine)
	}
}
