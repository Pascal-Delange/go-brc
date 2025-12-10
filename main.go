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
	"time"
	"unsafe"
)

func main() {
	s := time.Now()
	defer func() {
		fmt.Printf("Finished after %s", time.Since(s))
	}()
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	heapprofile := flag.String("heapprofile", "", "write heap profile to file")
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
	if *heapprofile != "" {
		f, err := os.Create(*heapprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
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
	resultMaps := make([]map[uint64]result, chunks)
	namesMaps := make([]map[uint64]string, chunks)
	for i = range chunks {
		idx := i
		resultMaps[idx] = make(map[uint64]result, 10_000)
		namesMaps[idx] = make(map[uint64]string, 10_000)
		wg.Go(func() {
			computeStatsMap(*fileName, int(idx), resultMaps[idx], namesMaps[idx], separators[idx], separators[idx+1], idx == 0)
		})
	}
	wg.Wait()

	out, names := computeStatsReduce(resultMaps, namesMaps)

	keysIter := maps.Keys(out)
	keys := slices.Sorted(keysIter)

	fmt.Print("{")
	ten := 10.
	first := true
	for _, k := range keys {
		if !first {
			fmt.Printf(", ")
		}
		first = false
		fmt.Printf("%s=%2.1f/%2.1f/%2.1f", names[k], float64(out[k].min)/ten, float64(out[k].sum)/float64(out[k].count)/ten, float64(out[k].max)/ten)
	}
	fmt.Print("}\n\n")
}

func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func computeStatsMap(fileName string, idx int, resultsMap map[uint64]result, namesMap map[uint64]string, start, end int64, first bool) {
	fmt.Printf("Chunk %d: Reading rows between bytes %d and %d\n", idx, start, end)
	tStart := time.Now()
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

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Fatalln(err)
		}

		byt = scanner.Bytes()

		sepIdx = bytes.IndexByte(byt, ';')

		nameAsInt := hashBytes(byt[:sepIdx])
		pointSeptIdx := bytes.IndexByte(byt[sepIdx+1:], '.')

		valStr := unsafeBytesToString(byt[sepIdx+1 : sepIdx+1+pointSeptIdx])

		val, _ = strconv.ParseInt(valStr, 0, 64)
		val = val*10 + int64(byt[sepIdx+1+pointSeptIdx+1])

		if _, ok := resultsMap[nameAsInt]; ok {
			resultsMap[nameAsInt] = result{
				count: resultsMap[nameAsInt].count + 1,
				sum:   resultsMap[nameAsInt].sum + val,
				min:   min(resultsMap[nameAsInt].min, val),
				max:   max(resultsMap[nameAsInt].max, val),
			}
		} else {
			namesMap[nameAsInt] = string(byt[:sepIdx])
			resultsMap[nameAsInt] = result{
				count: 1,
				sum:   val,
				min:   val,
				max:   val,
			}
		}

		ptr = ptr + int64(len(byt))
		if ptr > end && end > 0 {
			fmt.Printf("%d: exit at ptr %d after %s\n", idx, ptr, time.Since(tStart))
			return
		}
	}
}

func computeStatsReduce(resultsMaps []map[uint64]result, namesMaps []map[uint64]string) (out map[uint64]result, names map[uint64]string) {
	for i := range len(resultsMaps) {
		fmt.Printf("size of map %d: %d\n", i, len(resultsMaps[i]))
		// fmt.Println(resultsMaps[i])
	}

	out = resultsMaps[0]
	names = namesMaps[0]
	for i := 1; i < len(resultsMaps); i++ {
		for k, v := range resultsMaps[i] {
			vout, ok := out[k]

			if ok {
				out[k] = result{
					count: vout.count + v.count,
					sum:   vout.sum + v.sum,
					min:   min(vout.min, v.min),
					max:   max(vout.max, v.max),
				}
			} else {
				out[k] = v
				names[k] = namesMaps[i][k]
			}
		}
	}

	return out, names
}

func hashBytes(b []byte) uint64 {
	var out uint64
	for _, v := range b {
		out = out*52 + uint64(v)
	}
	return out
}
