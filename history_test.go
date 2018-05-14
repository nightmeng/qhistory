package qhistory

import (
	"testing"
	"time"
)

func TestHistory(t *testing.T) {
	h, err := NewHistory("history")
	if err != nil {
		t.Fatalf("create cache failed, %s\n", err)
	}
	defer h.Close()

	ts := time.Now()

	record := &Record{
		Data: []byte("hello"),
		Time: ts,
	}

	if err := h.Add([]byte("test"), record, 2*time.Second); err != nil {
		t.Fatalf("add record failed, %s\n", err)
	}

	if records, err := h.Scan([]byte("test"), time.Unix(0, 0), 5); err != nil {
		t.Fatalf("scan failed, %s\n", err)
	} else {
		if len(records) != 1 {
			t.Fatalf("scan failed, length: %d, expect: 1\n", len(records))
		}

		record := records[0]

		if string(record.Data) != "hello" {
			t.Fatalf("scan failed, invalid data: %s, expect: hello\n", len(record.Data))
		}

		if ts.UnixNano() != record.Time.UnixNano() {
			t.Fatalf("scan failed, invalid timestamp: %v, expect: %v\n", record.Time, ts)
		}
	}

	time.Sleep(2 * time.Second)
	if records, err := h.Scan([]byte("test"), time.Unix(0, 0), 5); err != nil {
		t.Fatalf("scan failed, %s\n", err)
	} else {
		if len(records) != 0 {
			t.Fatalf("scan failed, TTL failed\n")
		}
	}
}
