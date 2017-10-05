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
	"time"
)

type FileInfo struct {
	Name      string
	Directory string
	Path      string
	Extension string
	SHA256    []byte
}

func main() {
	var jobCount int
	flag.IntVar(&jobCount, "jobs", runtime.NumCPU(), "Number of jobs to run to calculate file information. Defaults to the number of CPUs on the system. You may want to set this to 1 for non-SSD drives.")
	// var outputPath string
	// flag.StringVar(&outputPath, "output", "", "File to write data to.")
	// TODO: Allow use of output path (not actually used yet)
	// TODO: Ensure at least one input path is provided
	// TODO: Ensure all root paths are valid paths
	// TODO: How is done channel supposed to be used?
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "You must provide at least 1 directory to scan\n")
		os.Exit(1)
	}

	output := csv.NewWriter(os.Stdout)
	output.Write([]string{
		"Name", "Directory", "Path", "Extension", "SHA256",
	})

	fmt.Fprintf(os.Stderr, "Calculating number of files...\n")
	fileCount := count(flag.Args()...)
	fmt.Fprintf(os.Stderr, "Found %v files to process\n", fileCount)

	pathChan := scan(flag.Args()...)
	done := make(chan struct{})
	defer close(done)

	infoChans := make([]<-chan FileInfo, jobCount)
	for i := 0; i < jobCount; i++ {
		infoChans[i] = proc(done, pathChan)
	}

	completedCount := 0
	startTime := time.Now()
	printStatsOnInterval(done, startTime, fileCount, &completedCount, 3*time.Second)

	for fi := range merge(done, infoChans...) {
		err := output.Write([]string{
			fi.Name,
			fi.Directory,
			fi.Path,
			fi.Extension,
			fmt.Sprintf("%x", fi.SHA256),
		})

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing CSV record\n")
		}

		completedCount++
	}

	printStats(startTime, float64(fileCount), float64(completedCount))
}

func proc(done <-chan struct{}, in <-chan string) <-chan FileInfo {
	out := make(chan FileInfo)
	go func() {
		defer close(out)
		for path := range in {
			f, err := os.Open(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nError opening file: %v\n", path)
				continue
			}

			abs, err := filepath.Abs(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nError calculating absolute path: %v\n", path)
				continue
			}

			hashSHA := sha256.New()
			io.Copy(hashSHA, f)
			fi := FileInfo{
				Name:      filepath.Base(path),
				Directory: filepath.Dir(path),
				Path:      abs,
				Extension: filepath.Ext(path),
				SHA256:    hashSHA.Sum(nil),
			}

			select {
			case out <- fi:
			case <-done:
				return
			}
		}
	}()
	return out
}

// Count the number of files in the given roots
func count(roots ...string) int {
	count := 0
	for _, root := range roots {
		filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() && err == nil {
				count++
			}
			return nil
		})
	}

	return count
}

func scan(roots ...string) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)

		for _, root := range roots {
			filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error walking: %+v\n", err)
				}
				if info.IsDir() {
					return nil
				}

				out <- path
				return nil
			})
		}
	}()
	return out
}

func merge(done <-chan struct{}, cs ...<-chan FileInfo) <-chan FileInfo {
	var wg sync.WaitGroup
	out := make(chan FileInfo)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan FileInfo) {
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
	remainingCount := fileCount - completedCount
	runTime := time.Since(startTime)
	fps := completedCount / float64(runTime/time.Second)
	percent := (completedCount / fileCount) * 100.0
	timeEstimate := time.Duration(remainingCount/fps) * time.Second
	fmt.Fprintf(os.Stderr, "\r  %v/%v processed  |  %.0f files/sec  |  %.0f%% complete  |  %v remaining", completedCount, fileCount, fps, percent, timeEstimate)
}

func printStatsOnInterval(done <-chan struct{}, startTime time.Time,
	fileCount int, completedCount *int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				printStats(startTime, float64(fileCount), float64(*completedCount))
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
}
