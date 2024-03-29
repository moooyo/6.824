package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	keyMap := make(map[string]*Intermediate)
	var KeyList []string
	var kvTemp Intermediate
	for i := 0; i < nMap; i = i + 1 {
		filename := reduceName(jobName, i, reduceTask)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		decoder := json.NewDecoder(file)
		for decoder.More() {
			err = decoder.Decode(&kvTemp)
			if err != nil {
				file.Close()
				fmt.Printf("intermediate decoder error\n")
				log.Fatal(err)
			}
			if keyMap[kvTemp.Key] == nil {
				KeyList = append(KeyList, kvTemp.Key)
				keyMap[kvTemp.Key] = new(Intermediate)
				keyMap[kvTemp.Key].Key = kvTemp.Key
			}
			for _, str := range kvTemp.Values {
				keyMap[kvTemp.Key].Values = append(keyMap[kvTemp.Key].Values, str)
			}
		}

		file.Close()
	}

	sort.Strings(KeyList)

	file, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)

	for _, str := range KeyList {
		var tmp *Intermediate
		var writeTmp KeyValue

		tmp = keyMap[str]
		if tmp == nil {
			continue
		}
		ret := reduceF(tmp.Key, tmp.Values)

		writeTmp.Key = tmp.Key
		writeTmp.Value = ret
		err = encoder.Encode(writeTmp)
		if err != nil {
			log.Fatal(err)
		}
	}

}
