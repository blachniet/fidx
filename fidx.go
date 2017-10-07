package main

import (
	"crypto/sha256"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type FileRecord struct {
	Name      string
	Directory string
	Path      string
	Extension string
	Size      int64
	SHA256    []byte
}

func main() {
	var jobCount int
	flag.IntVar(&jobCount, "jobs", runtime.NumCPU(), "Number of jobs to run to calculate file information. Defaults to the number of CPUs on the system. You may want to set this to 1 for non-SSD drives.")
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "You must provide at least 1 directory to scan\n")
		os.Exit(1)
	}

	output := csv.NewWriter(os.Stdout)
	output.Write([]string{
		"Name", "Directory", "Path", "Extension", "Size", "SHA256",
	})

	toProcessChan := scan(flag.Args()...)
	done := make(chan struct{})
	defer close(done)

	processedChan := make([]<-chan FileRecord, jobCount)
	for i := 0; i < jobCount; i++ {
		processedChan[i] = proc(done, toProcessChan)
	}

	var fileCount int64
	count(&fileCount, flag.Args()...)

	var completedCount int64
	startTime := time.Now()
	printStatsOnInterval(done, startTime, &fileCount, &completedCount, 3*time.Second)

	for fi := range merge(done, processedChan...) {
		err := output.Write([]string{
			fi.Name,
			fi.Directory,
			fi.Path,
			fi.Extension,
			fmt.Sprintf("%d", fi.Size),
			fmt.Sprintf("%x", fi.SHA256),
		})

		if err != nil {
			fmt.Fprintf(os.Stderr, "\rError writing CSV record\n")
		}

		completedCount++
	}

	printStats(startTime, float64(fileCount), float64(completedCount))
}

func proc(done <-chan struct{}, in <-chan FileRecord) <-chan FileRecord {
	out := make(chan FileRecord)
	go func() {
		defer close(out)
		for rec := range in {
			f, err := os.Open(rec.Path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\rError opening file: %v\n", rec.Path)
				continue
			}

			hashSHA := sha256.New()
			io.Copy(hashSHA, f)
			rec.SHA256 = hashSHA.Sum(nil)

			select {
			case out <- rec:
			case <-done:
				return
			}
		}
	}()
	return out
}

// count asynchronously walks the directories specified by
// roots and counts the number of files found. It stores
// the number of files in the provided integer.
func count(count *int64, roots ...string) {
	go func() {
		for _, root := range roots {
			filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err == nil && !info.IsDir() {
					atomic.AddInt64(count, 1)
				}
				return nil
			})
		}
	}()
}

// scan asynchronously walks the directories specified by
// roots and emits the paths to files found in the returned
// channel.
func scan(roots ...string) <-chan FileRecord {
	out := make(chan FileRecord)
	go func() {
		defer close(out)

		for _, root := range roots {
			filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					fmt.Fprintf(os.Stderr, "\rError walking: %+v\n", err)
				} else if !info.IsDir() {
					absPath, err := filepath.Abs(path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "\rError calculating absolute path: %v\n", path)
						return nil
					}

					out <- FileRecord{
						Name:      info.Name(),
						Directory: filepath.Dir(absPath),
						Path:      absPath,
						Extension: filepath.Ext(path),
						Size:      info.Size(),
					}
				}

				return nil
			})
		}
	}()
	return out
}

// merge "fans-in" the given channels
func merge(done <-chan struct{}, cs ...<-chan FileRecord) <-chan FileRecord {
	var wg sync.WaitGroup
	out := make(chan FileRecord)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan FileRecord) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-done:
				return
			}
		}
	}

	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func printStats(startTime time.Time, fileCount float64, completedCount float64) {
	runTime := time.Since(startTime)
	fps := completedCount / float64(runTime/time.Second)
	percent := (completedCount / fileCount) * 100.0
	fmt.Fprintf(os.Stderr, "\r  %v  | %v of %v processed  |  %.0f files/sec  |   %.1f%% complete          ", runTime, completedCount, fileCount, fps, percent)
}

func printStatsOnInterval(done <-chan struct{}, startTime time.Time,
	fileCount *int64, completedCount *int64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				printStats(startTime, float64(*fileCount), float64(*completedCount))
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
}
