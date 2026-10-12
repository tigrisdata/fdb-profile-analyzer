package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// testEvent is one line of `go test -json` output.
type testEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

type testResult struct {
	Name    string  `json:"name"`
	Package string  `json:"package"`
	Action  string  `json:"action"` // pass | fail | skip
	Elapsed float64 `json:"elapsed_seconds"`
	Output  string  `json:"output,omitempty"`
}

type summary struct {
	StartedAt time.Time    `json:"started_at"`
	Duration  float64      `json:"duration_seconds"`
	Total     int          `json:"total"`
	Passed    int          `json:"passed"`
	Failed    int          `json:"failed"`
	Skipped   int          `json:"skipped"`
	Results   []testResult `json:"results"`
}

// runReport turns a `go test -json` stream into a readable summary and a
// machine-readable file, and exits non-zero if anything failed — so it can sit
// at the end of a pipeline without swallowing the test exit code.
func runReport(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	in := fs.String("in", "-", "go test -json stream to read, or - for stdin")
	outDir := fs.String("out", "test-results", "directory to write results into")
	name := fs.String("name", "integration", "basename for the result files")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var r io.Reader = os.Stdin
	if *in != "-" {
		f, err := os.Open(*in)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}

	sum, err := parse(r)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	jsonPath := filepath.Join(*outDir, *name+".json")
	f, err := os.Create(jsonPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	writeErr := enc.Encode(sum)
	f.Close()
	if writeErr != nil {
		return writeErr
	}

	text := render(sum)
	txtPath := filepath.Join(*outDir, *name+".txt")
	if err := os.WriteFile(txtPath, []byte(text), 0o644); err != nil {
		return err
	}

	fmt.Print(text)
	fmt.Printf("\nresults written to %s and %s\n", txtPath, jsonPath)

	if sum.Failed > 0 {
		return fmt.Errorf("%d test(s) failed", sum.Failed)
	}
	return nil
}

func parse(r io.Reader) (*summary, error) {
	sum := &summary{}
	output := map[string][]string{} // test key -> captured output

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue // non-JSON noise on the stream is not fatal
		}
		if sum.StartedAt.IsZero() && !ev.Time.IsZero() {
			sum.StartedAt = ev.Time
		}
		if !ev.Time.IsZero() {
			sum.Duration = ev.Time.Sub(sum.StartedAt).Seconds()
		}
		if ev.Test == "" {
			continue // package-level events
		}

		key := ev.Package + "." + ev.Test
		switch ev.Action {
		case "output":
			output[key] = append(output[key], ev.Output)
		case "pass", "fail", "skip":
			res := testResult{Name: ev.Test, Package: ev.Package, Action: ev.Action, Elapsed: ev.Elapsed}
			if ev.Action == "fail" {
				res.Output = strings.Join(output[key], "")
			}
			sum.Results = append(sum.Results, res)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	for _, r := range sum.Results {
		sum.Total++
		switch r.Action {
		case "pass":
			sum.Passed++
		case "fail":
			sum.Failed++
		case "skip":
			sum.Skipped++
		}
	}
	sort.Slice(sum.Results, func(i, j int) bool { return sum.Results[i].Name < sum.Results[j].Name })
	return sum, nil
}

func render(s *summary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\nIntegration results — %d passed, %d failed, %d skipped in %.1fs\n",
		s.Passed, s.Failed, s.Skipped, s.Duration)
	fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 64))

	for _, r := range s.Results {
		mark := map[string]string{"pass": "ok  ", "fail": "FAIL", "skip": "skip"}[r.Action]
		fmt.Fprintf(&b, "  %s  %-46s %6.2fs\n", mark, r.Name, r.Elapsed)
	}

	// Failure detail last, so it is the final thing on screen.
	for _, r := range s.Results {
		if r.Action != "fail" || r.Output == "" {
			continue
		}
		fmt.Fprintf(&b, "\n--- %s\n", r.Name)
		for _, line := range strings.Split(strings.TrimRight(r.Output, "\n"), "\n") {
			fmt.Fprintf(&b, "    %s\n", line)
		}
	}
	return b.String()
}
