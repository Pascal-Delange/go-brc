package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"unsafe"
)

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	fileName := flag.String("filename", "./data/measurements_1M.txt", "file to read")
	routines := flag.Int("routines", runtime.NumCPU()+1, "nb of goroutines")
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

	fi, err := os.Stat(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	length := fi.Size()
	chunks := int64(*routines)
	fmt.Printf("Running with %d goroutines (and as many chunks)\n", chunks)
	chunksize := length / chunks
	separators := []int64{}
	var i int64
	for i = 0; i < chunks; i++ {
		separators = append(separators, i*chunksize)
	}
	separators = append(separators, -1)

	var wg sync.WaitGroup
	resultMaps := make([]map[string]result, chunks)
	for i = range chunks {
		idx := i
		resultMaps[idx] = make(map[string]result, 10_000)
		wg.Go(func() {
			computeStatsMap(*fileName, int(idx), resultMaps[idx], separators[idx], separators[idx+1], idx == 0)
		})
	}
	wg.Wait()

	out := computeStatsReduce(resultMaps)

	keysIter := maps.Keys(out)
	keys := slices.Sorted(keysIter)

	fmt.Print("{")
	ten := 10.
	for _, k := range keys {
		fmt.Printf("%s=%2.1f/%2.1f/%2.1f, ", k, float64(out[k].min)/ten, float64(out[k].sum)/float64(out[k].count)/ten, float64(out[k].max)/ten)
	}
	fmt.Print("}\n\n")
}

func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func computeStatsMap(fileName string, idx int, resultsMap map[string]result, start, end int64, first bool) {
	fmt.Printf("Chunk %d: Reading rows between bytes %d and %d\n", idx, start, end)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(start, io.SeekStart)
	reader := bufio.NewReader(file)
	ptr := start

	scanner := bufio.NewScanner(reader)
	if !first {
		more := scanner.Scan()
		if !more {
			return
		}
	}

	var byt []byte
	var val int64
	var sepIdx int
	i := 0
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Fatalln(err)
		}

		byt = scanner.Bytes()

		sepIdx = bytes.IndexByte(byt, ';')

		name := unsafeBytesToString(byt[:sepIdx])
		pointSeptIdx := bytes.IndexByte(byt[sepIdx+1:], '.')
		valStr := unsafeBytesToString(append(byt[sepIdx+1:sepIdx+1+pointSeptIdx], byt[sepIdx+1+pointSeptIdx+1:]...))
		// fmt.Println(valStr)

		// valStr = strings.Replace(valStr, ".", "", 1)
		val, _ = strconv.ParseInt(valStr, 0, 64)
		// fmt.Println("chunk: ", idx, name, val)

		if _, ok := resultsMap[name]; ok {
			// if textSl[0] == "Aachen" && valI > 990 {
			// 	fmt.Println(text)
			// }
			// if os.Getenv("log_level") == "debug" {
			// 	fmt.Printf("%s: \n - Before: %+v\n - received %v\n", textSl[0], resultsMap[textSl[0]], valI)
			// }
			resultsMap[name] = result{
				count: resultsMap[name].count + 1,
				sum:   resultsMap[name].sum + val,
				min:   min(resultsMap[name].min, val),
				max:   max(resultsMap[name].max, val),
			}
			// if os.Getenv("log_level") == "debug" {
			// 	fmt.Printf(" - new: %+v\n", resultsMap[textSl[0]])
			// }
		} else {
			resultsMap[name] = result{
				count: 1,
				sum:   val,
				min:   val,
				max:   val,
			}
		}
		// fmt.Println(resultsMap)
		if i > 300 {
			break
		} else {
			i++
		}

		ptr = ptr + int64(len(byt))
		if ptr > end && end > 0 {
			fmt.Printf("%d: exit at ptr %d\n", idx, ptr)
			return
		}
	}
}

func computeStatsReduce(resultsMaps []map[string]result) map[string]result {
	for i := range len(resultsMaps) {
		fmt.Printf("size of map %d: %d\n", i, len(resultsMaps[i]))
		// fmt.Println(resultsMaps[i])
	}

	out := resultsMaps[0]
	for i := 1; i < len(resultsMaps); i++ {
		for k, v := range resultsMaps[i] {
			// fmt.Println("k", k)
			vout, ok := out[k]
			// fmt.Println("vout", vout, "ok", ok)
			// k = string(k)
			if ok {
				out[k] = result{
					count: vout.count + v.count,
					sum:   vout.sum + v.sum,
					min:   min(vout.min, v.min),
					max:   max(vout.max, v.max),
				}
			} else {
				out[k] = v
			}
		}
	}
	// fmt.Println(out)
	// fmt.Println(resultsMaps[1])
	return out
}
