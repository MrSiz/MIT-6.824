package main

import (
	"fmt"
	"../mapreduce"
	"os"

	"strings"
	"strconv"
	"unicode"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you have to write this function
	//mapreduce.debug("Map %v\n", value)
	words := strings.FieldsFunc(value, func(t rune) bool {
		return !unicode.IsLetter(t);
	})

	temp := make(map[string]int)
	for _, w := range words {
		//kv := mapreduce.KeyValue{w, ""}
		//res = append(res, kv)
		if _, ok := temp[w]; !ok{
			temp[w] = 1
		}else {
			temp[w] += 1
		}
	}
	//if _, ok := temp["he"]; ok {
	//	fmt.Printf("he is %d\n", temp["he"])
	//}
	for k, v := range temp {
		kv := mapreduce.KeyValue{k, strconv.Itoa(v)}
		res = append(res, kv)
	}
	return
}


// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you also have to write this function

	sum := 0

	for _, str := range values {
		val, _ := strconv.Atoi(str)
		sum += val
	}
	return strconv.Itoa(sum)
}
// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
