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
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	workDir, err := os.Getwd()
	if err != nil {
		logger.Fatal(err.Error(), zap.Error(err))
	}

	var snapshotDir string
	var stableDir string
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

	serverID := flag.Arg(0)
	rpcServerAddr := flag.Arg(1)
	apiServerAddr := flag.Arg(2)

	snapshotDir = raft.PathJoin(workDir, snapshotDir)
	stableDir = raft.PathJoin(workDir, stableDir)

	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		logger.Fatal(err.Error(), zap.Error(err))
	}
	if err := os.MkdirAll(stableDir, 0755); err != nil {
		logger.Fatal(err.Error(), zap.Error(err))
	}

	logger.Info(fmt.Sprintf("using %s as directory for snapshot files", snapshotDir))
	logger.Info(fmt.Sprintf("using %s as directory for stable storage files", stableDir))

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
		raft.StableStorePathOption(filepath.Join(stableDir, fmt.Sprintf("stable_%s.db", serverID))),
	)

	if err := server.Serve(); err != nil {
		logger.Fatal(err.Error(), zap.Error(err))
	}
}
