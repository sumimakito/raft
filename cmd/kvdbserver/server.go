package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/sumimakito/raft"
	"github.com/sumimakito/raft/msgpackrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logLevels = map[string]zapcore.Level{
	"debug":  zap.DebugLevel,
	"info":   zap.InfoLevel,
	"warn":   zap.WarnLevel,
	"error":  zap.ErrorLevel,
	"dpanic": zap.DPanicLevel,
	"panic":  zap.PanicLevel,
	"fatal":  zap.FatalLevel,
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Panic(err)
	}

	workDir, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}

	var logLevelName string
	var snapshotDir string
	var stableDir string
	flag.StringVar(&logLevelName, "logLevel", "info",
		"Specifies the logging level (available: debug, info, warn, error, dpanic, panic, fatal).")
	flag.StringVar(&snapshotDir, "snapshotDir", filepath.Join(workDir, "snapshot"),
		"Specifies the directory that snapshot files are kept in.")
	flag.StringVar(&stableDir, "stableDir", filepath.Join(workDir, "stable"),
		"Specifies the directory that stable storage files are kept in.")

	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Printf("Usage: %s [options...] <server ID> <RPC server address> <API server address>\n", os.Args[0])
		fmt.Println()
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	logLevel, ok := logLevels[logLevelName]
	if !ok {
		log.Panicf("unknown log level: %s\n", logLevelName)
	}

	serverID := flag.Arg(0)
	rpcServerAddr := flag.Arg(1)
	apiServerAddr := flag.Arg(2)

	snapshotDir = raft.PathJoin(workDir, snapshotDir)
	stableDir = raft.PathJoin(workDir, stableDir)

	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		log.Panic(err)
	}
	if err := os.MkdirAll(stableDir, 0755); err != nil {
		log.Panic(err)
	}

	log.Printf("using %s as directory for snapshot files\n", snapshotDir)
	log.Printf("using %s as directory for stable storage files\n", stableDir)

	listener := raft.Must2(net.Listen("tcp", rpcServerAddr)).(net.Listener)
	kvdbAPIExt := NewAPIExtension(logger)
	logStore := NewLogStore(filepath.Join(stableDir, fmt.Sprintf("log_%s.db", serverID)))
	kvsm := NewKVSM()
	transport := msgpackrpc.NewTransport(listener)
	snapshot := NewSnapshotStore(snapshotDir)

	server := raft.NewServer(
		raft.ServerCoreOptions{
			ID:           raft.ServerID(serverID),
			Log:          logStore,
			StateMachine: kvsm,
			Snapshot:     snapshot,
			Transport:    transport,
		},
		raft.APIExtensionOption(kvdbAPIExt),
		raft.APIServerListenAddressOption(apiServerAddr),
		raft.LogLevelOption(logLevel),
		raft.StableStorePathOption(filepath.Join(stableDir, fmt.Sprintf("stable_%s.db", serverID))),
	)

	if err := server.Serve(); err != nil {
		log.Panic(err)
	}
}
