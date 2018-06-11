package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	merge := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		rf, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			log.Fatalf("os.Open error(%v)", err)
		}
		defer rf.Close()
		dec := json.NewDecoder(rf)
		for {
			var kvs []KeyValue
			if err := dec.Decode(&kvs); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("dec.Decode error(%v)", err)
			}

			for _, kv := range kvs {
				if _, ok := merge[kv.Key]; !ok {
					merge[kv.Key] = make([]string, 0)
				}
				merge[kv.Key] = append(merge[kv.Key], kv.Value)
			}
		}
	}

	mergeFile, err := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("os.OpenFile error(%v)", err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for k, v := range merge {
		if err := enc.Encode(KeyValue{k, reduceF(k, v)}); err != nil {
			log.Fatalf("enc.Encode error(%v)", err)
		}
	}
}
