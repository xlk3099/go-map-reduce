package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskID int, // which reduce task id
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("[reduce-%d], %v", reduceTaskID, err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = []string{}
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p := mergeName(jobName, reduceTaskID)
	resultFile, err := os.Create(p)
	if err != nil {
		log.Fatalf("[reduce-%d], %v", reduceTaskID, err)
	}
	defer resultFile.Close()
	enc := json.NewEncoder(resultFile)
	for _, k := range keys {
		res := reduceF(k, kvs[k])
		enc.Encode(KeyValue{k, res})
	}
}
