package raft

import (
	"log"
	"os"
	"path/filepath"
	"time"
)

type serverOptions struct {
	apiServerListenAddress    string
	apiExtensions             []APIExtension
	stableStorePath           string
	electionTimeout           time.Duration
	followerTimeout           time.Duration
	maxTimerRandomOffsetRatio float64
	metricsExporter           MetricsExporter
}

type ServerOption func(options *serverOptions)

func defaultServerOptions() *serverOptions {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return &serverOptions{
		apiServerListenAddress:    "",
		apiExtensions:             []APIExtension{},
		stableStorePath:           filepath.Join(wd, "stable.db"),
		electionTimeout:           1000 * time.Millisecond,
		followerTimeout:           1000 * time.Millisecond,
		maxTimerRandomOffsetRatio: 0.3,
		metricsExporter:           nil,
	}
}

func applyServerOpts(opts ...ServerOption) *serverOptions {
	options := defaultServerOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func APIServerListenAddressOption(address string) ServerOption {
	return func(options *serverOptions) {
		options.apiServerListenAddress = address
	}
}

func ElectionTimeoutOption(timeout time.Duration) ServerOption {
	return func(options *serverOptions) {
		options.electionTimeout = timeout
	}
}

func MetricsKeeperOption(exporter MetricsExporter) ServerOption {
	return func(options *serverOptions) {
		options.metricsExporter = exporter
	}
}

func APIExtensionOption(extension APIExtension) ServerOption {
	return func(options *serverOptions) {
		options.apiExtensions = append(options.apiExtensions, extension)
	}
}

func StableStorePathOption(path string) ServerOption {
	return func(options *serverOptions) {
		options.stableStorePath = path
	}
}
