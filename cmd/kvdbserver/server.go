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

	var stableDir string
	flag.StringVar(&stableDir, "stableDir", workDir, "Specifies the directory that stable storage files are kept in.")

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

	stableDir = raft.PathJoin(workDir, stableDir)

	stableDirFileInfo, err := os.Stat(stableDir)
	if err != nil {
		logger.Fatal(err.Error(), zap.Error(err))
	}
	if !stableDirFileInfo.IsDir() {
		logger.Fatal("path specified by -stableDir is not a directory")
	}

	logger.Info(fmt.Sprintf("using %s as directory for stable storage files", stableDir))

	listener := raft.Must2(net.Listen("tcp", rpcServerAddr)).(net.Listener)
	kvdbAPIExt := NewAPIExtension(logger)
	logStore := NewLogStore(filepath.Join(stableDir, fmt.Sprintf("log_%s.db", serverID)))
	kvsm := NewKVSM()
	transport := msgpackrpc.NewTransport(listener)

	server := raft.NewServer(raft.ServerID(serverID), logStore, kvsm, transport,
		raft.APIExtensionOption(kvdbAPIExt),
		raft.APIServerListenAddressOption(apiServerAddr),
		raft.StableStorePathOption(filepath.Join(stableDir, fmt.Sprintf("stable_%s.db", serverID))),
	)

	if err := server.Serve(); err != nil {
		logger.Fatal(err.Error(), zap.Error(err))
	}
}
