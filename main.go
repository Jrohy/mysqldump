package main

import (
	"flag"
	"fmt"
	common "mysqldump/common"
	xlog "mysqldump/xlog"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const Pattern = `\w+:\w+@\w+:\d{0,5}$`

var (
	flagChunksize, flagThreads, flagPort, flagStmtSize                                      int
	flagUser, flagPasswd, flagHost, flagSource, flagDb, flagTable, flagDir, flagSessionVars string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the dump")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDb, "db", "", "Database to dump")
	flag.StringVar(&flagTable, "table", "", "Table to dump")
	flag.StringVar(&flagDir, "o", "", "Directory to output files to")
	flag.IntVar(&flagChunksize, "F", 128, "Split tables into chunks of this output file size. This value is in MB")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.IntVar(&flagStmtSize, "s", 1000000, "Attempted size of INSERT statement in bytes")
	flag.StringVar(&flagSessionVars, "vars", "", "Session variables")
	flag.StringVar(&flagSource, "m", "", "Mysql source info in one string, format: user:password@ip:port")

	if flagSource != "" {
		if check, _ := regexp.Match(Pattern, []byte(flagSource)); check {
			sourceSlice := strings.Split(flagSource, "@")
			userSlice := strings.Split(sourceSlice[0], ":")
			addressSlice := strings.Split(sourceSlice[1], ":")
			flagUser, flagPasswd = userSlice[0], userSlice[1]
			flagHost = addressSlice[0]
			flagPort, _ = strconv.Atoi(addressSlice[1])
		}
	}
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR] -m [MYSQL_SOURCE]")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()

	if flagHost == "" || flagUser == "" || flagDb == "" {
		usage()
		os.Exit(0)
	}

	if _, err := os.Stat(flagDir); os.IsNotExist(err) {
		x := os.MkdirAll(flagDir, 0777)
		common.AssertNil(x)
	}

	args := &common.Args{
		User:          flagUser,
		Password:      flagPasswd,
		Address:       fmt.Sprintf("%s:%d", flagHost, flagPort),
		Database:      flagDb,
		Table:         flagTable,
		Outdir:        flagDir,
		ChunksizeInMB: flagChunksize,
		Threads:       flagThreads,
		StmtSize:      flagStmtSize,
		IntervalMs:    10 * 1000,
		SessionVars:   flagSessionVars,
	}

	Dumper(log, args)
}
