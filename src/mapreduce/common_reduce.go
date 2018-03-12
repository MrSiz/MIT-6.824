package mapreduce

import (
	//"os"
	//"encoding/json"
	//"fmt"
	//"log"
	//"fmt"
	"os"
	"log"
	"encoding/json"
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


	var help = make(map[string][]string)
	//fmt.Println("===========================")
	//fmt.Println(reduceTaskNumber)
	for i := 0; i < nMap; i++ {
		intmediateFileName := reduceName(jobName, i, reduceTaskNumber)
		fp, err := os.Open(intmediateFileName)
		if err != nil {
			log.Fatal("ewrw", err)
			continue
		}
		//else {
		//	fmt.Println("open the file successfully")
		//}
		defer fp.Close()

		dec := json.NewDecoder(fp)
		for {
			var v KeyValue
			err := dec.Decode(&v)
			if err != nil {
				break
			}
			//fmt.Println("the key is in reduce %s", v.Key)
			help[v.Key] = append(help[v.Key], v.Value)
		}
		//fmt.Printf("the index i is %d\n", i)
	}



	outFileName := mergeName(jobName, reduceTaskNumber)
	//fmt.Printf("the reduce fileName is %s\n", outFileName)
	file, _ := os.OpenFile(outFileName, os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()

	enc := json.NewEncoder(file)

	//var testString string
	//fmt.Println(len(help))
	for k, v := range help {
		//enc := json.NewEncoder(file)
		//err := enc.Encode(&kv)
		enc.Encode(KeyValue{k, reduceF(k, v)})
		//testString = k
		//fmt.Printf("enter in range map\n");
		//fmt.Println(k)
	}
	//fmt.Printf("the final string in reduce is %s\n", testString)

}
