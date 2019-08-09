package main

import (
	"flag"
	"fmt"
	"mysqldump/common"
	xlog "mysqldump/xlog"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const Pattern = `\w+:\w+@[\w.]+:\d{0,5}$`

var (
	flagOverwriteTables                                                                                          bool
	flagChunksize, flagThreads, flagPort, flagStmtSize                                                           int
	flagUser, flagPasswd, flagHost, flagSource, flagDb, flagTable, flagOutputDir, flagInputDir, flagExcludeTable string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the dump")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDb, "db", "", "Database to dump or database to import")
	flag.StringVar(&flagTable, "table", "", "Table to dump")
	flag.StringVar(&flagOutputDir, "o", "", "Directory to output files to")
	flag.StringVar(&flagInputDir, "i", "", "Directory of the dump to import")
	flag.IntVar(&flagChunksize, "F", 128, "Split tables into chunks of this output file size. This value is in MB")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.IntVar(&flagStmtSize, "s", 1000000, "Attempted size of INSERT statement in bytes")
	flag.StringVar(&flagSource, "m", "", "Mysql source info in one string, format: user:password@host:port")
	flag.StringVar(&flagExcludeTable, "exclude", "", "Do not dump the specified table data, use ',' to split multiple table")
	flag.BoolVar(&flagOverwriteTables, "d", false, "Drop tables if they already exist(import dump mode)")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR] -i [INDIR] -m [MYSQL_SOURCE] -exclude [EXCLUDE_TABLE]")
	flag.PrintDefaults()
	os.Exit(0)
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()
	var flagDir string

	if flagSource != "" {
		if check, _ := regexp.Match(Pattern, []byte(flagSource)); check {
			sourceSlice := strings.Split(flagSource, "@")
			userSlice := strings.Split(sourceSlice[0], ":")
			addressSlice := strings.Split(sourceSlice[1], ":")
			flagUser, flagPasswd = userSlice[0], userSlice[1]
			flagHost = addressSlice[0]
			flagPort, _ = strconv.Atoi(addressSlice[1])
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
	}

	args := &common.Args{
		User:            flagUser,
		Password:        flagPasswd,
		Address:         fmt.Sprintf("%s:%d", flagHost, flagPort),
		Database:        flagDb,
		Table:           flagTable,
		Outdir:          flagDir,
		ChunksizeInMB:   flagChunksize,
		Threads:         flagThreads,
		StmtSize:        flagStmtSize,
		IntervalMs:      10 * 1000,
		OverwriteTables: flagOverwriteTables,
		ExcludeTables:   flagExcludeTable,
	}

	if flagOutputDir != "" {
		Dumper(log, args)
	} else {
		Loader(log, args)
	}
}
