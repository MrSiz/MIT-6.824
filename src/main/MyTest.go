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
	t := make(chan int)
	go func() {
		t <- 1
	}()
	a := <- t
	fmt.Println(a)
}
