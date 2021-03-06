package main

import "os"
import "fmt"
import (
	"../mapreduce"
	"strings"
	"unicode"
	"sort"
	"strconv"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
type CntAndPosArr struct {
	cnt int
	pos []string
}


func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you should complete this to do the inverted index challenge
	//分词
	words := strings.FieldsFunc(value, func(t rune) bool {
		return !unicode.IsLetter(t);
	})

	flag := make(map[string]bool)
	store := make(map[string]string)
	for _, w := range words {
		//存每个单词对应的文档
		if _, exist := flag[w]; !exist {
			flag[w] = true
			store[w] = document
		}else {
			continue
		}
	}
	//返回KeyValue类型的切片
	for k, v := range store {
		res = append(res, mapreduce.KeyValue{k, v})
	}

	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you should complete this to do the inverted index challenge
	//出现在多少个文档中
	cnt := len(values)
	res := strconv.Itoa(cnt) + " "
	//排序所有的文档名
	sort.Strings(values)
	//存到[]string中
	for i, str := range values {
		if i != cnt - 1 {
			res = res + str + ","
		}else {
			res = res + str
		}
	}

	return res
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
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
