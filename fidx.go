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
		duration := time.Since(startTime)
		fps := float64(completedCount)/float64(duration/time.Second)
		percent := (float64(completedCount)/float64(fileCount))*100.0
		fmt.Fprintf(os.Stderr, "\r  %.0f%% complete  |  %v/%v files processed  |  %.0f files/sec", percent, completedCount, fileCount, fps)
	}
}

func proc(done <-chan struct{}, in <-chan string) <-chan FileInfo {
	out := make(chan FileInfo)
	go func() {
		defer close(out)
		for path := range in {
			f, err := os.Open(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening file: %v\n", path)
				continue
			}

			abs, err := filepath.Abs(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error calculating absolute path: %v\n", path)
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
			if !info.IsDir() && err == nil{
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