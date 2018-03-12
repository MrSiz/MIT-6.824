package main

import "fmt"

func main() {
	help := make(map[string][]string)
	help["1"] = append(help["1"], "a")
	help["1"] = append(help["1"], "b")

	for k, v := range help {
		fmt.Println(k, v)
	}
}
