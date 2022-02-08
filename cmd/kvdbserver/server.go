package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/sumimakito/raft"
	"github.com/sumimakito/raft/grpctrans"
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
	var pprofAddr string
	var snapshotDir string
	var stableDir string
	flag.StringVar(&logLevelName, "logLevel", "info",
		"Specifies the logging level (available: debug, info, warn, error, dpanic, panic, fatal).")
	flag.StringVar(&pprofAddr, "pprof", "",
		"Address for pprof to listen on.")
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

	if pprofAddr != "" {
		go func() {
			log.Printf("pprof will listen on %s\n", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Panic(err)
			}
		}()
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

	transport, err := grpctrans.NewTransport(rpcServerAddr)
	if err != nil {
		log.Panic(err)
	}
	kvdbAPIExt := NewAPIExtension(logger)
	logProvider := NewLogProvider(filepath.Join(stableDir, fmt.Sprintf("log_%s.db", serverID)))
	kvsm := NewKVSM()
	snapshot := NewSnapshotProvider(snapshotDir)

	server, err := raft.NewServer(
		raft.ServerCoreOptions{
			Id:               serverID,
			LogProvider:      logProvider,
			StateMachine:     kvsm,
			SnapshotProvider: snapshot,
			Transport:        transport,
		},
		raft.ElectionTimeoutOption(1*time.Second),
		raft.FollowerTimeoutOption(1*time.Second),
		raft.APIExtensionOption(kvdbAPIExt),
		raft.APIServerListenAddressOption(apiServerAddr),
		raft.LogLevelOption(logLevel),
		raft.StableStorePathOption(filepath.Join(stableDir, fmt.Sprintf("stable_%s.db", serverID))),
	)
	if err != nil {
		log.Panic(err)
	}

	if err := server.Serve(); err != nil {
		log.Panic(err)
	}
}
