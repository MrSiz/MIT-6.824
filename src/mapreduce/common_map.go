package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	//get the inFile content
	datArr, err := ioutil.ReadFile(inFile)

	if err != nil {
		fmt.Printf("error to read file\n")
		return 
	}
	var datString = string(datArr)


	keyValueData := mapF(inFile, datString)

	
	var flag  = make(map[string]*os.File)
	var encFlag = make(map[string]*json.Encoder)
	//var testString string
	//index := len(keyValueData)
	//fmt.Printf("index %d key %s key first %s\n", index, keyValueData[index - 1].Key, keyValueData[0].Key)

	var enc *json.Encoder
	for _, kv := range keyValueData {
		//fmt.Printf("%d\t%d\n", mapTaskNumber, (int(ihash(kv.Key)) % nReduce + nReduce) % nReduce)
		outName := reduceName(jobName, mapTaskNumber, (int(ihash(kv.Key)) % nReduce + nReduce) % nReduce)
		//fmt.Printf("the file Name is %s\n", outName)
		var file *os.File
		if _, ok := flag[outName]; !ok {
			file, err = os.OpenFile(outName, os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				log.Fatal("check: ", err)

			}
			defer file.Close()
			enc = json.NewEncoder(file)
			//fmt.Printf("the outName is %s enc is %v\n", outName, enc)
			flag[outName] = file
			//fmt.Printf("hhh the file Name is %s\n", outName)
			encFlag[outName] = enc
		}else {
			file = flag[outName]
			enc = encFlag[outName]
		}
		//fmt.Printf("Key %s : Value %s enc is %v\n", kv.Key, kv.Value, enc)
		//if (kv.Key == "99998") {
		//	//fmt.Printf("the initial enc is %v\n", enc)
		//	break
		//}
    	err := enc.Encode(&kv)

    	if err != nil {
        	log.Println("Error in encoding json")
    	}
    	//testString = kv.Key
	}

	//fmt.Printf("the final key is %s\n", testString)
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
