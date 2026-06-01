package log

import (
	"context"
	"io"
	"testing"
	"time"
)

type testPlatformWriter struct {
	levels []Level
}

func (w *testPlatformWriter) WriteMessage(level Level, _ string) {
	w.levels = append(w.levels, level)
}

func TestPlatformWriterHonorsConfiguredLevel(t *testing.T) {
	writer := new(testPlatformWriter)
	factory := NewDefaultFactory(
		context.Background(),
		Formatter{
			BaseTime:         time.Unix(0, 0),
			DisableTimestamp: true,
			DisableColors:    true,
		},
		io.Discard,
		"",
		writer,
		false,
	)
	factory.SetLevel(LevelInfo)

	logger := factory.Logger()
	logger.Debug("hidden")
	logger.Info("shown")

	if len(writer.levels) != 1 || writer.levels[0] != LevelInfo {
		t.Fatalf("platform writer levels=%v, want [%d]", writer.levels, LevelInfo)
	}
}
