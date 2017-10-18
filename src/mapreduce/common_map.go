package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// step 1, read the file
	data, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}
	// step 2, pass the content to the map function
	kvs := mapF(inFile, string(data))
	// Step 3: create intermediate files
	var tmpFiles []*os.File = make([]*os.File, nReduce)
	var encoders []*json.Encoder = make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		tmpFileName := reduceName(jobName, mapTaskNumber, i)
		tmpFiles[i], err = os.Create(tmpFileName)
		if err != nil {
			log.Fatal("domap error when creating file ", err)
		}
		defer tmpFiles[i].Close()
		encoders[i] = json.NewEncoder(tmpFiles[i])
	}
	for _, kv := range kvs {
		hashKey := int(ihash(kv.Key)) % nReduce
		err := encoders[hashKey].Encode(kv)
		if err != nil {
			log.Fatal("domap encoders", err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
