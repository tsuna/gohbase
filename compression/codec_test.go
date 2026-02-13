package compression

import "testing"

func TestNewCodec(t *testing.T) {
	tcs := []struct {
		codec       string
		shouldPanic bool
	}{
		{"snappy", false},
		{"someCodec", true},
	}
	for _, tc := range tcs {
		func() {
			defer func() {
				r := recover()
				if !tc.shouldPanic {
					if r != nil {
						t.Errorf("Expected New(%q) to not panic, but it did", tc.codec)
					}
				} else {
					if r == nil {
						t.Errorf("Expected New(%q) to panic, but it did not", tc.codec)
					}
				}
			}()
			New(tc.codec)
		}()
	}
}
