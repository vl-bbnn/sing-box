//go:build with_wlt && wlt_deadline_fixture

package wlt_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRetainedEOFActualCore4484(t *testing.T) {
	root := os.Getenv("WLT_DEADLINE_LIFECYCLE_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("external output required")
	}
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprintf("iteration-%02d", i), func(t *testing.T) {
			for _, tc := range []struct {
				name string
				run  func(*testing.T)
			}{{"request", TestDeadlineLifecycle4479RequestDeadlinePreserved}, {"manager", TestDeadlineLifecycle4479ActualManagerEOF}} {
				t.Run(tc.name, func(t *testing.T) {
					d := filepath.Join(root, fmt.Sprintf("iteration-%02d-%s", i, tc.name))
					if e := os.Mkdir(d, 0700); e != nil {
						t.Fatal(e)
					}
					t.Setenv("WLT_DEADLINE_LIFECYCLE_ROOT", d)
					tc.run(t)
				})
			}
		})
	}
}
