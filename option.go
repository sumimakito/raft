package raft

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap/zapcore"
)

type serverOptions struct {
	apiServerListenAddress    string
	apiExtensions             []APIExtension
	stableStorePath           string
	electionTimeout           time.Duration
	followerTimeout           time.Duration
	logLevel                  zapcore.Level
	maxTimerRandomOffsetRatio float64
	metricsExporter           MetricsExporter
	snapshotPolicy            SnapshotPolicy
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
		logLevel:                  zapcore.InfoLevel,
		maxTimerRandomOffsetRatio: 0.3,
		metricsExporter:           nil,
		snapshotPolicy:            SnapshotPolicy{Applies: 10, Interval: 1 * time.Second},
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

func FollowerTimeoutOption(timeout time.Duration) ServerOption {
	return func(options *serverOptions) {
		options.followerTimeout = timeout
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

func LogLevelOption(level zapcore.Level) ServerOption {
	return func(options *serverOptions) {
		options.logLevel = level
	}
}

func StableStorePathOption(path string) ServerOption {
	return func(options *serverOptions) {
		options.stableStorePath = path
	}
}

func SnapshotPolicyOption(policy SnapshotPolicy) ServerOption {
	return func(options *serverOptions) {
		options.snapshotPolicy = policy
	}
}
