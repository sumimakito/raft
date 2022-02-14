package main

import (
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
	"github.com/sumimakito/raft/grpctrans"
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

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Panic(err)
	}

	workDir, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}

	var clusterConfig string
	var logLevelName string
	var pprofAddr string
	var snapshotDir string
	var stableDir string
	flag.StringVar(&clusterConfig, "cluster", "",
		"Path to the cluster config file.")
	flag.StringVar(&logLevelName, "logLevel", "info",
		"Logging level (available: debug, info, warn, error, dpanic, panic, fatal).")
	flag.StringVar(&pprofAddr, "pprof", "",
		"Address for pprof to listen on.")
	flag.StringVar(&snapshotDir, "snapshotDir", filepath.Join(workDir, "snapshot"),
		"Directory for snapshot files.")
	flag.StringVar(&stableDir, "stableDir", filepath.Join(workDir, "stable"),
		"Directory for stable storage files.")

	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Printf("Usage: %s [OPTIONS] <SERVER_ID> <RPC_ADDRESS> <API_ADDRESS>\n", os.Args[0])
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
	apiExtension := NewAPIExtension(logger)
	logProvider := raft.NewBoltLogProvider(filepath.Join(stableDir, fmt.Sprintf("log_%s.db", serverID)))
	stateMachine := NewStateMachine()
	snapshotProvider := NewSnapshotProvider(snapshotDir)

	server, err := raft.NewServer(
		raft.ServerCoreOptions{
			Id:               serverID,
			InitialCluster:   cluster,
			LogProvider:      logProvider,
			StateMachine:     stateMachine,
			SnapshotProvider: snapshotProvider,
			Transport:        transport,
		},
		raft.ElectionTimeoutOption(1*time.Second),
		raft.FollowerTimeoutOption(1*time.Second),
		raft.APIExtensionOption(apiExtension),
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