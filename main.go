package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
)

func main() {
	computeStats()
}

type result struct {
	count int
	sum   int
	max   int
	min   int
}

func computeStats() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	fileName := flag.String("filename", "./data/measurements_1M.txt", "file to read")
	flag.Parse()
	fmt.Println("reading file " + *fileName)
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	file, err := os.Open(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	resultsMap := make(map[string]result, 10_000)
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Fatalln(err)
		}
		text := scanner.Text()
		textSl := strings.Split(text, ";")
		textSl[1] = strings.Replace(textSl[1], ".", "", 1)
		val, _ := strconv.ParseInt(textSl[1], 0, 64)
		valI := int(val)
		if _, ok := resultsMap[textSl[0]]; ok {
			resultsMap[textSl[0]] = result{
				count: resultsMap[textSl[0]].count + 1,
				sum:   resultsMap[textSl[0]].sum + valI,
				min:   min(resultsMap[textSl[0]].min, valI),
				max:   max(resultsMap[textSl[0]].max, valI),
			}
		} else {
			resultsMap[textSl[0]] = result{
				count: 1,
				sum:   valI,
				min:   valI,
				max:   valI,
			}
		}
	}
}
