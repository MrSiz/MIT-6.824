package main

import (
	"fmt"
	//"log"
	"hash/fnv"
)

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func main() {
	help := make(map[string][]string)
	help["1"] = append(help["1"], "a")
	help["1"] = append(help["1"], "b")

	for k, v := range help {
		fmt.Println(k, v)
	}
	for k := range help {
		fmt.Println(k)
	}

	fmt.Println(ihash("he"))
	fmt.Println(ihash("he"))
	//log.
}
