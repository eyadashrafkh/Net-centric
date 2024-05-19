package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
		return
	}

	fileSize := fileInfo.Size()
	fileContent := make([]byte, fileSize)
	_, err = file.Read(fileContent)
	if err != nil {
		log.Fatal(err)
		return
	}

	keyvals := mapFn(inputFile, string(fileContent))

	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := getIntermediateName(jobName, mapTaskIndex, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer file.Close()
		encoders[i] = json.NewEncoder(file)
	}

	for _, keyval := range keyvals {
		reduceTaskIndex := int(hash32(keyval.Key)) % nReduce
		err := encoders[reduceTaskIndex].Encode(keyval)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {
	keyvals := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := getIntermediateName(jobName, i, reduceTaskIndex)
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
		if err != nil {
			log.Fatal(err)
			return
		}
		decoder := json.NewDecoder(file)
		var keyval KeyValue
		for decoder.Decode(&keyval) == nil {
			keyvals[keyval.Key] = append(keyvals[keyval.Key], keyval.Value)
		}
		defer file.Close()
	}
	
	file := getReduceOutName(jobName, reduceTaskIndex)
	outputFile, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
		return
	}
	
	var keys []string
	for key := range keyvals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	
	encoder := json.NewEncoder(outputFile)
	for _, key := range keys {
		value := keyvals[key]
		output := reduceFn(key, value)
		err := encoder.Encode(KeyValue{Key: key, Value: output})
		if err != nil {
			log.Fatal(err)
		}
	}
	defer outputFile.Close()
}
