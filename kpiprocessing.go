package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"example.com/kpi-processing/kpiapphelper"
	"example.com/kpi-processing/models"
	"example.com/kpi-processing/parquethelper/dataprocessor"
	_ "github.com/marcboeker/go-duckdb"
)

// main entry point
func main() {
	//creating the arguments for passing to .net dll
	kpiAppArgs := kpiapphelper.NewKpiAppArgs()
	cmdName, cmdArgs := kpiAppArgs.GetCommandLine()

	//creating process for .net dll
	cmd := exec.Command(cmdName, cmdArgs...)

	startTime := time.Now()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("creating output pipe failed: %v", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("creating error pipe failed: %v", err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatalf("starting command failed: %v", err)
	}

	//channels for batch processing
	metadataChan := make(chan []models.KpiInfo, dataprocessor.BatchSize)
	kpiDataChan := make(chan models.KPIDataArgs, dataprocessor.BatchSize)
	done := make(chan struct{})
	wg := sync.WaitGroup{}

	//starting the batch data processor
	go dataprocessor.ProcessBatches(kpiDataChan, metadataChan, done)

	readPipe := func(pipe io.ReadCloser, label string) {
		defer wg.Done()
		reader := bufio.NewReader(pipe)

		for {
			select {
			case <-done:
				return
			default:
				line, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						return
					}
					// logging error
					if !strings.Contains(err.Error(), "file already closed") {
						log.Printf("error reading in %s: %v", label, err)
					}
					return
				}

				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				//code for getting meta data
				//then creating schema using this
				if strings.HasPrefix(line, "MetaData-") {
					jsonData := strings.TrimPrefix(line, "MetaData-")
					var kpiData []models.KpiInfo
					err := json.Unmarshal([]byte(jsonData), &kpiData)
					if err != nil {
						log.Printf("metadata json parsing error: %v", err)
						continue
					}
					select {
					case metadataChan <- kpiData:
					case <-done:
						return
					}
				} else if strings.HasPrefix(line, "KpiData-") { //code for getting kpi data and then inserting it database
					jsonData := strings.TrimPrefix(line, "KpiData-")
					var kpiDataArgs models.KPIDataArgs
					err := json.Unmarshal([]byte(jsonData), &kpiDataArgs)
					if err != nil {
						log.Printf("kpidata json parsing error: %v", err)
						continue
					}
					select {
					case kpiDataChan <- kpiDataArgs:
					case <-done:
						return
					}
				} else {
					fmt.Printf("[%s] %s\n", label, line)
				}
			}
		}
	}

	wg.Add(2)
	go readPipe(stdout, "STDOUT")
	go readPipe(stderr, "STDERR")

	// waiting for dotnet process to complete
	if err := cmd.Wait(); err != nil {
		log.Printf("command complete with error: %v", err)
	}

	wg.Wait()

	//closing kpi data channels
	close(metadataChan)
	close(kpiDataChan)

	//final all process done
	<-done

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("Time Taken: %.2f seconds\n", elapsed)
}
