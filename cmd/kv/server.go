package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/sumimakito/raft"
	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
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

type parsedClusterConfig struct {
	Peers map[string]string `yaml:"peers"`
}

func ensureDir(dir string) error {
	if stat, err := os.Stat(dir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	} else if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	return nil
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

	var apiAddress string
	var clusterConfig string
	var logLevelName string
	var pprofAddr string
	flag.StringVar(&apiAddress, "api", "",
		"Address for API server to listen on.")
	flag.StringVar(&clusterConfig, "cluster", "",
		"Path to the cluster config file.")
	flag.StringVar(&logLevelName, "log", "info",
		"Logging level (available: debug, info, warn, error, dpanic, panic, fatal).")
	flag.StringVar(&pprofAddr, "pprof", "",
		"Address for pprof to listen on.")

	flag.Parse()

	if flag.NArg() < 3 {
		fmt.Printf("Usage: %s [OPTIONS] <SERVER_ID> <RPC_ADDRESS> <DATA_DIR>\n", os.Args[0])
		fmt.Println()
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	var cluster []*pb.Peer
	if clusterConfig != "" {
		func() {
			file, err := os.Open(raft.PathJoin(workDir, clusterConfig))
			if err != nil {
				log.Panic(err)
			}
			defer file.Close()
			b, err := ioutil.ReadAll(file)
			if err != nil {
				log.Panic(err)
			}
			var c parsedClusterConfig
			yaml.Unmarshal(b, &c)
			for id, endpoint := range c.Peers {
				cluster = append(cluster, &pb.Peer{Id: id, Endpoint: endpoint})
			}
		}()
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
		log.Panicf("unknown log level: %s", logLevelName)
	}

	serverID := flag.Arg(0)
	rpcServerAddr := flag.Arg(1)
	dataDirArg := flag.Arg(2)

	dataDir := raft.PathJoin(workDir, dataDirArg)
	if err := ensureDir(dataDir); err != nil {
		log.Panic(err)
	}

	snapshotsDir := filepath.Join(dataDir, "snapshots")
	if err := ensureDir(snapshotsDir); err != nil {
		log.Panic(err)
	}

	transport, err := raft.NewGRPCTransport(rpcServerAddr)
	if err != nil {
		log.Panic(err)
	}
	apiExtension := NewAPIExtension(logger)
	stableStore, err := raft.NewBoltStore(filepath.Join(dataDir, "store.db"))
	if err != nil {
		log.Panic(err)
	}
	stateMachine := NewStateMachine()
	snapshotStore := NewSnapshotStore(snapshotsDir)

	serverOpts := []raft.ServerOption{
		raft.ElectionTimeoutOption(1 * time.Second),
		raft.FollowerTimeoutOption(1 * time.Second),
		raft.APIExtensionOption(apiExtension),
		raft.LogLevelOption(logLevel),
	}

	if apiAddress != "" {
		serverOpts = append(serverOpts, raft.APIServerListenAddressOption(apiAddress))
	}

	server, err := raft.NewServer(
		raft.ServerCoreOptions{
			Id:             serverID,
			InitialCluster: cluster,
			StableStore:    stableStore,
			StateMachine:   stateMachine,
			SnapshotStore:  snapshotStore,
			Transport:      transport,
		},
		serverOpts...,
	)
	if err != nil {
		log.Panic(err)
	}

	if err := server.Serve(); err != nil {
		log.Panic(err)
	}
}
