package mapreduce

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

const debugEnabled = true
const outTestPath = "tmp_testout552"

// The function will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase jobPhase = "Reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// getIntermediateName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func getIntermediateName(jobName string, mapTask int, reduceTask int) string {
	return filepath.Join(outTestPath, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

// getReduceOutName constructs the name of the output file of reduce task <reduceTask>
func getReduceOutName(jobName string, reduceTask int) string {
	return filepath.Join(outTestPath, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}

func getChildrenFiles(parentDir string) []string {
	files := make([]string, 0)
	entries, err := os.ReadDir(parentDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		files = append(files, filepath.Join(parentDir, e.Name()))
	}
	return files
}
