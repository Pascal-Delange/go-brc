package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"time"
)

var bufSizeMb int

func main() {
	s := time.Now()
	defer func() {
		fmt.Printf("Finished after %s", time.Since(s))
	}()
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	heapprofile := flag.String("heapprofile", "", "write heap profile to file")
	fileName := flag.String("filename", "./data/measurements_1M.txt", "file to read")
	routines := flag.Int("routines", runtime.NumCPU()+1, "nb of goroutines")
	bufsizeFlag := flag.Int("buffer", 5, "buffer size in Mb")
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
	bufSizeMb = *bufsizeFlag

	fi, err := os.Stat(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	length := int(fi.Size())
	chunks := *routines
	fmt.Printf("Running with %d goroutines (and as many chunks)\n", chunks)
	chunksize := length / chunks
	separators := []int{}
	var i int
	for i = 0; i < chunks; i++ {
		separators = append(separators, i*chunksize)
	}
	separators = append(separators, length)

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
	resByName := make(map[string]result, 1_000)
	for k, v := range names {
		resByName[v] = out[k]
	}

	keysIter := maps.Keys(resByName)
	keys := slices.Sorted(keysIter)

	fmt.Print("{")
	ten := 10.
	first := true
	for _, k := range keys {
		if !first {
			fmt.Printf(", ")
		}
		first = false
		fmt.Printf("%s=%2.1f/%2.1f/%2.1f", k, float64(resByName[k].min)/ten, float64(resByName[k].sum)/float64(resByName[k].count)/ten, float64(resByName[k].max)/ten)
	}
	fmt.Print("}\n\n")
}

func computeStatsMap(fileName string, idx int, resultsMap map[uint64]result, namesMap map[uint64]string, start, end int, first bool) {
	var pos int
	startT := time.Now()
	defer func() {
		val := recover()
		if val != nil {
			fmt.Println("in", idx, "at position", pos, "recovered:", val)
			panic(val)
		}
		fmt.Printf("Goroutine %d finished in %2.2fs\n", idx, time.Since(startT).Seconds())
	}()
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(start), io.SeekStart)

	bytesToProcess := end - start
	buffer := make([]byte, bufSizeMb*1024*1024)

	var val int64
	var nameAsInt uint64
	var mult int64
	nameMode := true
	ptr := 0
	var nameStart, nameEnd int

	cont := true

	n, _ := file.Read(buffer)
	validLength := n
	// fmt.Println(idx, ": initially read ", n, " bytes")

	firstIteration := true

	for cont {
		lastCompleteRow := 0
		// find the end of the last complete row, looking only at the valid part of the buffer (which may be less than buffer length at the end of processing)
		for i := validLength - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				lastCompleteRow = i
				break
			}
		}
		// fmt.Println(idx, ": last complete row stops at:", lastCompleteRow)
		bytesThisLoop := 0

		// itereate up to end of last complete row in the buffer
		for i := 0; i < lastCompleteRow; {
			if firstIteration && !first {
				for j := 0; ; j++ {
					if buffer[i+j] == '\n' {
						i = i + j + 1

						break
					}
				}
				firstIteration = false
			}
			// if idx == 1 {
			// 	fmt.Println("i", i)
			// }
			switch nameMode {
			case true:
				sep := 0
				for j := 0; ; j++ {
					// if idx == 1 {
					// 	fmt.Println("i+j", i+j)
					// }
					pos = i
					if buffer[i+j] == ';' {
						sep = j
						break
					}
				}
				nameStart, nameEnd = i, i+sep
				nameAsInt = hashBytes(buffer[i : i+sep])
				nameMode = false
				val = 0
				mult = 1
				i += sep + 1
				continue
			case false:
				if buffer[i] == '-' {
					mult = -1
					i++
				}
				val = int64(buffer[i] - '0')
				i++
				// ptr is now on the char after the first number
				if buffer[i] != '.' {
					// not a point so it's a XX.X number
					val = 100*val + 10*int64(buffer[i]-'0') + int64(buffer[i+2]-'0')
					i += 4
				} else {
					// we got a point, so it's a X.X number
					val = 10*val + int64(buffer[i+1]-'0')
					i += 3
				}
				// in either case, place i on the char after the next \n
				val = val * mult

				resultsMap[nameAsInt] = newResult(resultsMap[nameAsInt], val)
				if resultsMap[nameAsInt].count == 1 {
					namesMap[nameAsInt] = string(buffer[nameStart:nameEnd])
				}

				nameMode = true

				if i+ptr > bytesToProcess {
					cont = false
					break
				}

				bytesThisLoop = i
				continue
			}
		}

		ptr += bytesThisLoop
		// fmt.Println(idx, ": new pointer at ", ptr)

		// this is the number of valid bytes remaining after the end of the last valid row
		remainingBytes := validLength - (lastCompleteRow + 1)

		// copy remaining bytes to beginning
		copy(buffer, buffer[lastCompleteRow+1:validLength])

		// and extend the buffer to its original size
		buffer = buffer[:cap(buffer)]

		// get more bytes
		n, err := file.Read(buffer[remainingBytes:])
		// fmt.Println(idx, ": read ", n, " bytes")
		if err == io.EOF {
			cont = false
		}
		// compute new valid length, based on the previous read
		validLength = remainingBytes + n
		// fmt.Println(idx, ": valid length:", validLength)
		buffer = buffer[:validLength]

	}
}

func newResult(old result, val int64) result {
	return result{
		count: old.count + 1,
		sum:   old.sum + val,
		min:   min(old.min, val),
		max:   max(old.max, val),
	}
}

func computeStatsReduce(resultsMaps []map[uint64]result, namesMaps []map[uint64]string) (out map[uint64]result, names map[uint64]string) {
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
