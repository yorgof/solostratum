package stats

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func at(t *testing.T, value string) time.Time {
	t.Helper()
	when, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return when
}

func history(t *testing.T, dir, month string) []Sample {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "history-"+month+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []Sample
	for sc := bufio.NewScanner(f); sc.Scan(); {
		var sample Sample
		if err := json.Unmarshal(sc.Bytes(), &sample); err != nil {
			t.Fatalf("history line %q: %v", sc.Text(), err)
		}
		out = append(out, sample)
	}
	return out
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func checkHistory(t *testing.T, got, want []Sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("history: %+v\nwant:    %+v", got, want)
	}
	for i := range want {
		if !got[i].Time.Equal(want[i].Time) || got[i].Worker != want[i].Worker || got[i].Counts != want[i].Counts {
			t.Errorf("line %d: %+v, want %+v", i+1, got[i], want[i])
		}
	}
}

func TestHistoryHasOneLinePerWorkerAndInterval(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "stats")
	s := open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:04:10Z"), 1024, 5000)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:04:59Z"), 2048, 3000)
	s.Rejected("bitaxe1", at(t, "2026-10-03T12:03:00Z"))
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:05:00Z"), 2048, 2500.5)
	s.Accepted("axe2", at(t, "2026-10-03T14:07:30+02:00"), 512, 600) // 12:07:30 UTC
	s.flush(at(t, "2026-10-03T12:10:00Z"), false)

	checkHistory(t, history(t, dir, "2026-10"), []Sample{
		{at(t, "2026-10-03T12:00:00Z"), "bitaxe1", Counts{Accepted: 2, Rejected: 1, Work: 3072, BestShare: 5000}},
		{at(t, "2026-10-03T12:05:00Z"), "axe2", Counts{Accepted: 1, Work: 512, BestShare: 600}},
		{at(t, "2026-10-03T12:05:00Z"), "bitaxe1", Counts{Accepted: 1, Work: 2048, BestShare: 2500.5}},
	})

	// The file format is a promise to whoever reads these files later.
	raw, _ := os.ReadFile(filepath.Join(dir, "history-2026-10.jsonl"))
	first, _, _ := strings.Cut(string(raw), "\n")
	if want := `{"t":"2026-10-03T12:00:00Z","worker":"bitaxe1","accepted":2,"rejected":1,"work":3072,"bestShare":5000,"connections":0,"online":0}`; first != want {
		t.Errorf("first line:\n  %s\nwant:\n  %s", first, want)
	}
}

// The interval in progress is written once it is over, or at shutdown.
func TestUnfinishedIntervalIsKeptBack(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:04:00Z"), 1, 1)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:06:00Z"), 2, 2)
	s.flush(at(t, "2026-10-03T12:07:00Z"), false)
	checkHistory(t, history(t, dir, "2026-10"), []Sample{
		{at(t, "2026-10-03T12:00:00Z"), "bitaxe1", Counts{Accepted: 1, Work: 1, BestShare: 1}},
	})
	// The totals are complete all the same.
	if got := open(t, dir).totals["bitaxe1"].Accepted; got != 2 {
		t.Errorf("accepted shares in the totals: %d, want 2", got)
	}

	s.Accepted("bitaxe1", at(t, "2026-10-03T12:08:00Z"), 4, 4)
	s.flush(at(t, "2026-10-03T12:09:00Z"), true)
	checkHistory(t, history(t, dir, "2026-10"), []Sample{
		{at(t, "2026-10-03T12:00:00Z"), "bitaxe1", Counts{Accepted: 1, Work: 1, BestShare: 1}},
		{at(t, "2026-10-03T12:05:00Z"), "bitaxe1", Counts{Accepted: 2, Work: 6, BestShare: 4}},
	})
}

func TestTotalsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:00:01.7Z"), 1024, 5000)
	s.Rejected("bitaxe1", at(t, "2026-10-03T12:01:00Z"))
	s.flush(at(t, "2026-10-03T12:02:00Z"), true)

	s = open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-04T08:30:00Z"), 4096, 4500)
	s.Accepted("other", at(t, "2026-10-04T08:31:00Z"), 1, 2)
	s.flush(at(t, "2026-10-04T08:32:00Z"), true)

	s = open(t, dir)
	if len(s.totals) != 2 {
		t.Fatalf("totals: %+v", s.totals)
	}
	got := s.totals["bitaxe1"]
	if got.Counts != (Counts{Accepted: 2, Rejected: 1, Work: 5120, BestShare: 5000}) {
		t.Errorf("counts: %+v", got.Counts)
	}
	if !got.FirstSeen.Equal(at(t, "2026-10-03T12:00:01Z")) || !got.LastSeen.Equal(at(t, "2026-10-04T08:30:00Z")) {
		t.Errorf("first seen %v, last seen %v", got.FirstSeen, got.LastSeen)
	}
	// The second run added to the history instead of replacing it.
	if lines := history(t, dir, "2026-10"); len(lines) != 3 {
		t.Errorf("history: %+v", lines)
	}
	for _, name := range files(t, dir) {
		if strings.HasSuffix(name, ".tmp") {
			t.Errorf("temporary file left behind: %s", name)
		}
	}
}

// A restart inside an interval leaves two lines for it. Together they must
// still be the truth.
func TestLinesOfOneIntervalAddUp(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:00:10Z"), 1024, 1500)
	s.flush(at(t, "2026-10-03T12:01:00Z"), true)
	s = open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T12:04:10Z"), 1024, 9000)
	s.flush(at(t, "2026-10-03T12:04:30Z"), true)

	lines := history(t, dir, "2026-10")
	if len(lines) != 2 {
		t.Fatalf("history: %+v", lines)
	}
	var sum Counts
	for _, line := range lines {
		if !line.Time.Equal(at(t, "2026-10-03T12:00:00Z")) {
			t.Errorf("unexpected interval %v", line.Time)
		}
		sum.add(line.Counts)
	}
	if sum != (Counts{Accepted: 2, Work: 2048, BestShare: 9000}) {
		t.Errorf("sum of the lines: %+v", sum)
	}
}

func TestEachMonthHasItsOwnFile(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-31T23:59:59Z"), 1, 1)
	s.Accepted("bitaxe1", at(t, "2026-11-01T00:00:00Z"), 1, 1)
	s.flush(at(t, "2026-11-01T00:00:01Z"), true)
	if oct, nov := history(t, dir, "2026-10"), history(t, dir, "2026-11"); len(oct) != 1 || len(nov) != 1 {
		t.Errorf("october %+v, november %+v", oct, nov)
	}
}

func TestNothingIsWrittenWithoutMiners(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.flush(at(t, "2026-10-03T12:00:00Z"), false)
	if names := files(t, dir); len(names) != 0 {
		t.Fatalf("files written without any miner: %v", names)
	}

	s.Accepted("bitaxe1", at(t, "2026-10-03T12:00:00Z"), 1, 1)
	s.flush(at(t, "2026-10-03T12:05:00Z"), false)
	before := files(t, dir)
	if err := os.Remove(filepath.Join(dir, totalsFile)); err != nil {
		t.Fatal(err)
	}
	s.flush(at(t, "2026-10-03T12:10:00Z"), false)
	if names := files(t, dir); len(names) != len(before)-1 {
		t.Errorf("an idle interval wrote to disk: %v", names)
	}
}

func TestConnectionsAndOnlineTime(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Connected("bitaxe1", at(t, "2026-10-03T12:02:30Z"))
	s.flush(at(t, "2026-10-03T12:05:00Z"), false)
	// A second connection under the same name does not count the time twice.
	s.Connected("bitaxe1", at(t, "2026-10-03T12:06:00Z"))
	s.Disconnected("bitaxe1", at(t, "2026-10-03T12:07:00Z"))
	s.flush(at(t, "2026-10-03T12:10:00Z"), false)
	s.Disconnected("bitaxe1", at(t, "2026-10-03T12:11:40Z"))
	// Gone for a while, then back.
	s.Connected("bitaxe1", at(t, "2026-10-03T12:13:00Z"))
	s.Disconnected("bitaxe1", at(t, "2026-10-03T12:13:20Z"))
	// A disconnect without a connect, as after hitting the worker limit.
	s.Disconnected("stranger", at(t, "2026-10-03T12:13:30Z"))
	s.flush(at(t, "2026-10-03T12:20:00Z"), false)

	checkHistory(t, history(t, dir, "2026-10"), []Sample{
		{at(t, "2026-10-03T12:00:00Z"), "bitaxe1", Counts{Connections: 1, Online: 150}},
		{at(t, "2026-10-03T12:05:00Z"), "bitaxe1", Counts{Connections: 1, Online: 300}},
		{at(t, "2026-10-03T12:10:00Z"), "bitaxe1", Counts{Connections: 1, Online: 120}},
	})
	got := open(t, dir).totals["bitaxe1"]
	if got.Counts != (Counts{Connections: 3, Online: 570}) {
		t.Errorf("totals: %+v", got.Counts)
	}
	if !got.FirstSeen.Equal(at(t, "2026-10-03T12:02:30Z")) || !got.LastSeen.Equal(at(t, "2026-10-03T12:13:20Z")) {
		t.Errorf("first seen %v, last seen %v", got.FirstSeen, got.LastSeen)
	}
	if len(s.live) != 0 {
		t.Errorf("workers still counted as connected: %v", s.live)
	}
}

// A miner that stays connected is seen right up to the last flush, also
// when it delivers no shares.
func TestConnectedWorkerIsSeenWithoutShares(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Connected("bitaxe1", at(t, "2026-10-03T12:00:00Z"))
	s.flush(at(t, "2026-10-03T13:00:00Z"), false)
	got := open(t, dir).totals["bitaxe1"]
	if got.Online != 3600 || !got.LastSeen.Equal(at(t, "2026-10-03T13:00:00Z")) {
		t.Errorf("online %d s, last seen %v", got.Online, got.LastSeen)
	}
	if lines := history(t, dir, "2026-10"); len(lines) != 12 {
		t.Errorf("%d history lines for an hour, want 12", len(lines))
	}
}

func TestOffline(t *testing.T) {
	s := open(t, t.TempDir())
	if got := s.Offline(); got == nil || len(got) != 0 {
		t.Fatalf("offline workers of an empty store: %#v, want an empty list", got)
	}
	s.Connected("old", at(t, "2026-10-01T08:00:00Z"))
	s.Accepted("old", at(t, "2026-10-01T08:10:00Z"), 16, 99)
	s.Disconnected("old", at(t, "2026-10-01T09:00:00Z"))
	s.Connected("gone", at(t, "2026-10-03T08:00:00Z"))
	s.Disconnected("gone", at(t, "2026-10-03T08:00:30Z"))
	s.Connected("here", at(t, "2026-10-03T11:00:00Z"))
	s.Connected("twice", at(t, "2026-10-03T11:00:00Z"))
	s.Connected("twice", at(t, "2026-10-03T11:01:00Z"))
	s.Disconnected("twice", at(t, "2026-10-03T11:02:00Z"))

	got := s.Offline()
	if len(got) != 2 || got[0].Worker != "gone" || got[1].Worker != "old" {
		t.Fatalf("offline workers: %+v", got)
	}
	if got[1].Counts != (Counts{Accepted: 1, Work: 16, BestShare: 99, Connections: 1, Online: 3600}) ||
		!got[1].LastSeen.Equal(at(t, "2026-10-01T09:00:00Z")) {
		t.Errorf("offline worker: %+v", got[1])
	}

	for i := 0; i < maxOffline+10; i++ {
		s.Rejected(fmt.Sprintf("crowd%d", i), at(t, "2026-09-01T00:00:00Z"))
	}
	if got := s.Offline(); len(got) != maxOffline || got[0].Worker != "gone" {
		t.Errorf("%d offline workers listed, the first is %q", len(got), got[0].Worker)
	}
}

func TestSeries(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	// "late" was first seen after "early", so it comes second whatever its name.
	s.Connected("zz-early", at(t, "2026-10-02T10:00:00Z"))
	s.Accepted("zz-early", at(t, "2026-10-02T11:59:00Z"), 100, 1) // before the period
	s.Accepted("zz-early", at(t, "2026-10-02T12:00:00Z"), 900, 7)
	s.Accepted("zz-early", at(t, "2026-10-02T12:14:59Z"), 900, 3)
	s.Accepted("zz-early", at(t, "2026-10-02T12:15:00Z"), 450, 2)
	s.Rejected("zz-early", at(t, "2026-10-02T12:16:00Z"))
	s.flush(at(t, "2026-10-02T12:30:00Z"), false) // these are read back from the file
	s.Connected("late", at(t, "2026-10-03T11:00:00Z"))
	s.Accepted("late", at(t, "2026-10-03T11:50:00Z"), 1800, 50)
	s.Accepted("late", at(t, "2026-10-03T12:01:00Z"), 5000, 60) // in the interval still in progress
	now := at(t, "2026-10-03T12:03:00Z")

	got, err := s.Series(now, 24*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Start.Equal(at(t, "2026-10-02T12:00:00Z")) || got.Step != 900 || got.Steps != 96 || !got.Since.Equal(got.Start) {
		t.Fatalf("series: start %v, step %d, %d steps, since %v", got.Start, got.Step, got.Steps, got.Since)
	}
	if len(got.Workers) != 2 || got.Workers[0].Worker != "zz-early" || got.Workers[1].Worker != "late" || got.Other != nil || got.Others != 0 {
		t.Fatalf("workers: %+v", got)
	}
	early, late := got.Workers[0], got.Workers[1]
	if len(early.Hashrate) != 96 || len(late.Hashrate) != 96 {
		t.Fatalf("%d and %d steps", len(early.Hashrate), len(late.Hashrate))
	}
	// 1800 difficulty in 900 seconds is two difficulty-1 shares a second.
	if early.Hashrate[0] != 2*4294967296 || early.Hashrate[1] != 0.5*4294967296 || early.Hashrate[2] != 0 {
		t.Errorf("early hashrate: %v", early.Hashrate[:3])
	}
	if late.Hashrate[95] != 2*4294967296 || late.Hashrate[94] != 0 {
		t.Errorf("late hashrate: %v", late.Hashrate[94:])
	}
	// Online from 12:00 the day before until the last full interval.
	if early.Counts != (Counts{Accepted: 3, Rejected: 1, Work: 2250, BestShare: 7, Online: 24 * 3600}) {
		t.Errorf("early counts: %+v", early.Counts)
	}
	if late.Counts != (Counts{Accepted: 1, Work: 1800, BestShare: 50, Connections: 1, Online: 3600}) {
		t.Errorf("late counts: %+v", late.Counts)
	}

	// Asked again within the same interval, the answer is not built anew.
	again, _ := s.Series(now.Add(time.Minute), 24*time.Hour, 15*time.Minute)
	if &again.Workers[0] != &got.Workers[0] {
		t.Error("the series was rebuilt within one interval")
	}
	// A new interval brings a new answer, with the interval that just ended.
	next, err := s.Series(now.Add(2*time.Minute), 24*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Start.Equal(at(t, "2026-10-02T12:05:00Z")) || next.Workers[1].Accepted != 2 {
		t.Errorf("next series: start %v, late %+v", next.Start, next.Workers[1].Counts)
	}
	// A longer period in wider steps holds the same shares.
	week, err := s.Series(now, 7*24*time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rate := week.Workers[0].Hashrate
	if week.Steps != 168 || week.Workers[0].Accepted != 4 ||
		rate[143] != math.Round(100*4294967296.0/3600) || rate[144] != math.Round(2250*4294967296.0/3600) {
		t.Errorf("week: %d steps, early %+v, hashrate %v", week.Steps, week.Workers[0].Counts, rate[143:145])
	}
}

// Where the statistics begin inside the period, the first step is averaged
// over the time that was recorded, not over the whole step.
func TestSeriesFromTheStartOfTheStatistics(t *testing.T) {
	s := open(t, t.TempDir())
	s.Connected("bitaxe1", at(t, "2026-10-03T11:50:00Z"))
	s.Accepted("bitaxe1", at(t, "2026-10-03T11:52:00Z"), 600, 1)
	s.Accepted("bitaxe1", at(t, "2026-10-03T11:56:00Z"), 300, 1)
	got, err := s.Series(at(t, "2026-10-03T12:00:00Z"), 7*24*time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Since.Equal(at(t, "2026-10-03T11:50:00Z")) {
		t.Fatalf("since %v", got.Since)
	}
	// 900 difficulty in the ten recorded minutes of the last hour.
	rate := got.Workers[0].Hashrate
	if rate[167] != math.Round(900*4294967296.0/600) || rate[166] != 0 {
		t.Errorf("hashrate: %v", rate[166:])
	}

	// A few seconds of history are not stretched into a wild figure.
	s = open(t, t.TempDir())
	s.Accepted("bitaxe1", at(t, "2026-10-03T11:59:58Z"), 60, 1)
	got, _ = s.Series(at(t, "2026-10-03T12:00:00Z"), 24*time.Hour, 15*time.Minute)
	if rate := got.Workers[0].Hashrate[95]; rate != 4294967296 {
		t.Errorf("hashrate from two seconds of history: %v, want it averaged over a minute", rate)
	}

	// Lines older than anything the totals know still count from where they start.
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "history-2026-10.jsonl"), []byte(`{"t":"2026-10-03T09:00:00Z","worker":"lost","accepted":1,"work":3600}`+"\n"), 0o644)
	s = open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T11:30:00Z"), 1, 1)
	got, _ = s.Series(at(t, "2026-10-03T12:00:00Z"), 24*time.Hour, time.Hour)
	if !got.Since.Equal(at(t, "2026-10-03T09:00:00Z")) || got.Other == nil || got.Other.Hashrate[21] != 4294967296 {
		t.Errorf("since %v, other %+v", got.Since, got.Other)
	}
}

func TestSeriesAddsUpWorkersBeyondTheLimit(t *testing.T) {
	s := open(t, t.TempDir())
	for i := 0; i < MaxSeries+3; i++ {
		// Seen for the first time one after the other.
		s.Accepted(fmt.Sprintf("worker%02d", i), at(t, "2026-10-03T11:00:00Z").Add(time.Duration(i)*time.Second), 900, float64(i))
	}
	s.Accepted("forgotten", at(t, "2026-08-01T00:00:00Z"), 1, 1) // not seen for more than 30 days
	got, err := s.Series(at(t, "2026-10-03T12:00:00Z"), 24*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Workers) != MaxSeries || got.Workers[0].Worker != "worker00" || got.Workers[MaxSeries-1].Worker != fmt.Sprintf("worker%02d", MaxSeries-1) {
		t.Fatalf("workers: %+v", got.Workers)
	}
	if got.Others != 3 || got.Other == nil {
		t.Fatalf("%d others: %+v", got.Others, got.Other)
	}
	if got.Other.Counts != (Counts{Accepted: 3, Work: 2700, BestShare: float64(MaxSeries + 2)}) || got.Other.Hashrate[92] != 3*4294967296 {
		t.Errorf("other: %+v, hashrate %v", got.Other.Counts, got.Other.Hashrate[92])
	}
}

func TestSeriesSkipsDamagedLines(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.Accepted("bitaxe1", at(t, "2026-10-03T11:00:00Z"), 900, 1)
	s.flush(at(t, "2026-10-03T11:10:00Z"), false)
	f, _ := os.OpenFile(filepath.Join(dir, "history-2026-10.jsonl"), os.O_WRONLY|os.O_APPEND, 0o644)
	f.WriteString(`{"t":"2026-10-03T11:05:00Z","worker":"bitaxe1","acc` + "\n")
	f.Close()
	s.Accepted("bitaxe1", at(t, "2026-10-03T11:20:00Z"), 900, 1)
	s.flush(at(t, "2026-10-03T11:30:00Z"), false)

	got, err := s.Series(at(t, "2026-10-03T12:00:00Z"), 24*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Workers) != 1 || got.Workers[0].Accepted != 2 {
		t.Errorf("series: %+v", got.Workers)
	}
}

func TestDamagedTotalsAreSetAside(t *testing.T) {
	for _, content := range []string{`{"bitaxe1": {"accepted": 12`, "null", ""} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, totalsFile), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		s := open(t, dir)
		if len(s.totals) != 0 {
			t.Errorf("%q: totals %+v", content, s.totals)
		}
		names := files(t, dir)
		if len(names) != 1 || !strings.HasPrefix(names[0], totalsFile+".damaged-") {
			t.Fatalf("%q: files %v, want only the damaged file under a new name", content, names)
		}
		if kept, _ := os.ReadFile(filepath.Join(dir, names[0])); string(kept) != content {
			t.Errorf("%q: the damaged file was changed to %q", content, kept)
		}
		// Recording works again straight away.
		s.Accepted("bitaxe1", at(t, "2026-10-03T12:00:00Z"), 1, 1)
		s.flush(at(t, "2026-10-03T12:01:00Z"), true)
		if again := open(t, dir); again.totals["bitaxe1"].Accepted != 1 {
			t.Errorf("%q: totals after recovery: %+v", content, again.totals)
		}
	}
}

func TestEmptyWorkerEntryIsDropped(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, totalsFile), []byte(`{"ghost": null, "bitaxe1": {"accepted": 3}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s := open(t, dir)
	if len(s.totals) != 1 || s.totals["bitaxe1"].Accepted != 3 {
		t.Fatalf("totals: %+v", s.totals)
	}
	s.Offline() // must not trip over the empty entry
}

func TestUnwritableFolder(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-folder")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file); err == nil {
		t.Fatal("a file was accepted as the statistics folder")
	}
}

func TestWorkerNamesAreLimited(t *testing.T) {
	s := open(t, t.TempDir())
	when := at(t, "2026-10-03T12:00:00Z")
	for i := 0; i < maxWorkers+5; i++ {
		s.Connected(fmt.Sprintf("worker%d", i), when)
	}
	if len(s.totals) != maxWorkers || len(s.pending) != maxWorkers || len(s.live) != maxWorkers {
		t.Fatalf("%d totals, %d pending lines and %d connected, want %d", len(s.totals), len(s.pending), len(s.live), maxWorkers)
	}
	// Workers that are already known keep being recorded.
	s.Accepted("worker0", when, 8, 8)
	if got := s.totals["worker0"].Counts; got != (Counts{Accepted: 1, Work: 8, BestShare: 8, Connections: 1}) {
		t.Errorf("known worker: %+v", got)
	}
}

func TestRunWritesOnShutdown(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := fmt.Sprintf("worker%d", i)
			s.Connected(worker, time.Now())
			for j := 0; j < 100; j++ {
				s.Accepted(worker, time.Now(), 1, 1)
				s.Offline()
				s.Series(time.Now(), time.Hour, Interval)
			}
		}()
	}
	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}

	again := open(t, dir)
	for i := 0; i < 4; i++ {
		if got := again.totals[fmt.Sprintf("worker%d", i)].Counts; got.Accepted != 100 || got.Connections != 1 {
			t.Errorf("worker%d on disk: %+v, want 100 accepted shares and 1 connection", i, got)
		}
	}
}

func TestHistoryRetriesAfterWriteFailure(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	when := at(t, "2026-10-03T12:01:00Z")
	s.Accepted("rig", when, 1024, 2048)
	path := filepath.Join(dir, historyFile(when))
	// A directory at the file path causes a write failure even as root.
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	s.flush(when.Add(10*time.Minute), false)
	if len(s.pending) != 1 {
		t.Fatal("failed samples were discarded")
	}
	// More work in the same bucket must be merged with the pending sample.
	s.Accepted("rig", when.Add(time.Second), 512, 4096)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.flush(when.Add(15*time.Minute), false)
	s.flush(when.Add(20*time.Minute), false)
	want := Counts{Accepted: 2, Work: 1536, BestShare: 4096}
	checkHistory(t, history(t, dir, "2026-10"), []Sample{{when.Truncate(Interval), "rig", want}})
	if got := open(t, dir).totals["rig"].Counts; got != want {
		t.Errorf("retries changed lifetime totals: %+v, want %+v", got, want)
	}
	if len(s.pending) != 0 {
		t.Fatal("written samples still pending")
	}
}

func TestHistoryRetryDoesNotDuplicateAnEarlierMonth(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	sept := at(t, "2026-09-30T23:59:00Z")
	oct := at(t, "2026-10-01T00:01:00Z")
	s.Accepted("rig", sept, 1, 1)
	s.Accepted("rig", oct, 2, 2)
	path := filepath.Join(dir, historyFile(oct))
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	s.flush(oct.Add(10*time.Minute), false)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.flush(oct.Add(15*time.Minute), false)
	checkHistory(t, history(t, dir, "2026-09"), []Sample{{sept.Truncate(Interval), "rig", Counts{Accepted: 1, Work: 1, BestShare: 1}}})
	checkHistory(t, history(t, dir, "2026-10"), []Sample{{oct.Truncate(Interval), "rig", Counts{Accepted: 1, Work: 2, BestShare: 2}}})
}

// shortHistoryFile injects a disk that writes part of a batch and may also
// refuse the attempt to remove its incomplete last line.
type shortHistoryFile struct {
	*os.File
	limit        int
	failTruncate bool
}

func (f shortHistoryFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p[:f.limit], off)
	if err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

func (f shortHistoryFile) Truncate(size int64) error {
	if f.failTruncate {
		return errors.New("disk unavailable")
	}
	return f.File.Truncate(size)
}

func TestHistoryRetriesPartialBatchesWithoutDuplicates(t *testing.T) {
	when := at(t, "2026-10-03T12:00:00Z")
	samples := []Sample{
		{when, "a", Counts{Accepted: 1, Work: 1}},
		{when, "b", Counts{Accepted: 1, Work: 2}},
	}
	var data []byte
	var firstEnd int
	for i, sample := range samples {
		line, err := json.Marshal(sample)
		if err != nil {
			t.Fatal(err)
		}
		data = append(append(data, line...), '\n')
		if i == 0 {
			firstEnd = len(data)
		}
	}
	for _, limit := range []int{0, 10, firstEnd, firstEnd + 10, len(data) - 1} {
		for _, failTruncate := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%d/truncateFails=%v", limit, failTruncate), func(t *testing.T) {
				dir := t.TempDir()
				s := open(t, dir)
				f, err := os.OpenFile(filepath.Join(dir, historyFile(when)), os.O_RDWR|os.O_CREATE, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				written, err := appendHistoryBatch(shortHistoryFile{f, limit, failTruncate}, data)
				f.Close()
				wantWritten := 0
				if limit >= firstEnd {
					wantWritten = 1
				}
				if err == nil || written != wantWritten {
					t.Fatalf("partial write: %d complete samples, error %v", written, err)
				}
				var got []Sample
				if err := s.readHistory(when, when.Add(Interval), func(sample Sample) { got = append(got, sample) }); err != nil {
					t.Fatal(err)
				}
				checkHistory(t, got, samples[:written])
				if rest, err := s.appendHistory(samples[written:]); err != nil || len(rest) != 0 {
					t.Fatalf("retry: %d samples unwritten, error %v", len(rest), err)
				}
				checkHistory(t, history(t, dir, "2026-10"), samples)
			})
		}
	}
}

func TestUnfinishedTailIsRepairedBeforeAppending(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	when := at(t, "2026-10-03T12:00:00Z")
	first := Sample{when, "a", Counts{Accepted: 1, Work: 1}}
	line, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	// A complete line followed by half of another: what a power cut leaves.
	torn := append(append(line, '\n'), line[:len(line)/2]...)
	if err := os.WriteFile(filepath.Join(dir, historyFile(when)), torn, 0o600); err != nil {
		t.Fatal(err)
	}
	next := Sample{when.Add(Interval), "a", Counts{Accepted: 2, Work: 2}}
	// Through the real file: the flags it is opened with must allow the
	// repair on every platform.
	if rest, err := s.appendHistory([]Sample{next}); err != nil || len(rest) != 0 {
		t.Fatalf("append after a torn write: %d samples unwritten, error %v", len(rest), err)
	}
	checkHistory(t, history(t, dir, "2026-10"), []Sample{first, next})
}

func TestUnwrittenHistoryIsBounded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	when := at(t, "2026-10-03T00:00:00Z")
	if err := os.Mkdir(filepath.Join(dir, historyFile(when)), 0o700); err != nil {
		t.Fatal(err)
	}
	// One worker mines through more intervals than can be kept, while the
	// disk refuses every write.
	const extra = 10
	intervals := maxPending + extra
	for i := range intervals {
		s.Accepted("rig", when.Add(time.Duration(i)*Interval), 1, 1)
	}
	s.flush(when.Add(time.Duration(intervals)*Interval), false)
	if len(s.pending) != maxPending {
		t.Fatalf("%d samples kept, want %d", len(s.pending), maxPending)
	}
	for i := range extra {
		if s.pending[sampleKey{when.Add(time.Duration(i) * Interval).Unix(), "rig"}] != nil {
			t.Fatalf("interval %d was kept although it is among the oldest", i)
		}
	}
	if got := s.totals["rig"].Accepted; got != uint64(intervals) {
		t.Fatalf("lifetime totals %d, want %d", got, intervals)
	}
}

func TestFlushAfterRunKeepsLateMinerEvents(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	start := time.Now().UTC().Truncate(time.Second)
	s.Connected("rig", start)
	s.Accepted("rig", start, 2, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Run(ctx)
	// The server drains after the history loop has stopped.
	s.Accepted("rig", start.Add(5*time.Second), 4, 8)
	s.Disconnected("rig", start.Add(10*time.Second))
	s.Flush(start.Add(11 * time.Second))
	s.Flush(start.Add(12 * time.Second)) // repeating the final flush is harmless
	want := Counts{Accepted: 2, Work: 6, BestShare: 8, Connections: 1, Online: 10}
	if got := open(t, dir).totals["rig"].Counts; got != want {
		t.Fatalf("persisted totals %+v, want %+v", got, want)
	}
	var sum Counts
	if err := s.readHistory(start.Add(-Interval), start.Add(24*time.Hour), func(sample Sample) { sum.add(sample.Counts) }); err != nil {
		t.Fatal(err)
	}
	if sum != want {
		t.Fatalf("history %+v, want %+v", sum, want)
	}
}

func TestTotalsRetryDoesNotDuplicateHistory(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	when := at(t, "2026-10-03T12:00:00Z")
	s.Accepted("rig", when, 1, 2)
	s.Flush(when)
	blocked := filepath.Join(dir, totalsFile+".tmp")
	if err := os.Mkdir(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	s.Accepted("rig", when.Add(time.Second), 4, 8)
	s.Flush(when.Add(2 * time.Second))
	if !s.changed || open(t, dir).totals["rig"].Accepted != 1 {
		t.Fatal("failed write changed disk totals or was not retained for retry")
	}
	if err := os.Remove(blocked); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	s.Flush(when.Add(3 * time.Second))
	if got := open(t, dir).totals["rig"].Counts; got != (Counts{Accepted: 2, Work: 5, BestShare: 8}) {
		t.Fatalf("recovered totals %+v", got)
	}
	if lines := history(t, dir, "2026-10"); len(lines) != 2 {
		t.Fatalf("retry duplicated history: %+v", lines)
	}
}

func TestFailedTailRepairDoesNotAppend(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	when := at(t, "2026-10-03T12:00:00Z")
	path := filepath.Join(dir, historyFile(when))
	if err := os.WriteFile(path, []byte("unfinished"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	n, err := appendHistoryBatch(shortHistoryFile{File: f, failTruncate: true}, []byte("new\n"))
	f.Close()
	if err == nil || n != 0 {
		t.Fatalf("failed repair committed %d lines, error %v", n, err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "unfinished" {
		t.Fatalf("failed repair modified file: %q, %v", raw, err)
	}
	sample := Sample{when, "rig", Counts{Accepted: 1}}
	if rest, err := s.appendHistory([]Sample{sample}); err != nil || len(rest) != 0 {
		t.Fatalf("retry: %d unwritten, %v", len(rest), err)
	}
	checkHistory(t, history(t, dir, "2026-10"), []Sample{sample})
}
