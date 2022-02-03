package raft

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func logFields(server *Server, keysAndValues ...interface{}) []interface{} {
	return append([]interface{}{
		zap.Any("server", server.Info()),
		zap.String("role", server.role().String()),
		zap.Uint64("term", server.currentTerm()),
		zap.Uint64("commit_index", server.commitIndex()),
		zap.Uint64("last_log_index", server.lastLogIndex()),
		zap.Uint64("last_applied_index", server.lastAppliedIndex()),
	}, keysAndValues...)
}

func wrappedServerLogger(logLevel zapcore.Level) *zap.SugaredLogger {
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel && lvl >= logLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel && lvl >= logLevel
	})

	consoleStdout := zapcore.Lock(os.Stdout)
	consoleStderr := zapcore.Lock(os.Stderr)

	prodEncoderConfig := zap.NewProductionEncoderConfig()
	develEncoderConfig := zap.NewDevelopmentEncoderConfig()
	develEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	jsonEncoder := zapcore.NewJSONEncoder(prodEncoderConfig)
	consoleEncoder := zapcore.NewConsoleEncoder(develEncoderConfig)

	_ = jsonEncoder
	// zapcore.AddSync(colorable.NewColorableStdout()),

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleStdout, lowPriority),
		zapcore.NewCore(consoleEncoder, consoleStderr, highPriority),
	)

	logger := zap.New(core)

	return logger.Sugar()
}
