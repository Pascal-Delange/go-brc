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

type bufferedReader struct {
	reader     io.Reader
	buf        []byte
	pos        int
	size       int
	endReached bool
	counter    int
	maxBytes   int
	start      time.Time
}

var x io.Reader

func NewBufferedReader(r io.Reader, size int, maxBytes int) bufferedReader {
	return bufferedReader{
		reader:   r,
		buf:      make([]byte, size),
		size:     size,
		maxBytes: maxBytes,
		start:    time.Now(),
	}
}
func (b *bufferedReader) ReadAByte() (p byte, done bool) {
	for {

		if b.pos < len(b.buf) {
			b.pos++
			return b.buf[b.pos-1], b.pos == len(b.buf) && b.endReached
		}

		b.pos = 0
		n, err := b.reader.Read(b.buf)
		cutPos := min(n, b.maxBytes-b.counter)

		// if time.Now().After(b.start.Add(time.Second * 20)) {
		// 	fmt.Println("cutpos", cutPos)
		// 	fmt.Println("n,err", n, err)
		// }
		b.counter += cutPos
		// if err == io.EOF || cutPos < len(b.buf) {
		if err == io.EOF || b.counter == b.maxBytes {
			b.endReached = true
		}
		b.buf = b.buf[:cutPos]
		if len(b.buf) == 0 {
			panic("endless loop")
		}
	}
}

func readUpToFirstSemColon(r bufferedReader) {
	for {
		p, _ := r.ReadAByte()
		if p == '\n' {
			return
		}
	}
}

func computeStatsMap(fileName string, idx int, resultsMap map[uint64]result, namesMap map[uint64]string, start, end int, first bool) {
	tStart := time.Now()
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(start), io.SeekStart)

	scanner := NewBufferedReader(file, bufSizeMb*1024*1024, end-start)

	if !first {
		readUpToFirstSemColon(scanner)
	}

	var val int64
	var sepIdx int
	var nameAsInt uint64
	var mult int64
	nameMode := true
	byt := make([]byte, 0, 100)

	cont := true
	for cont {
		b, done := scanner.ReadAByte()
		if done {
			fmt.Printf("%d: exit at after %s\n", idx, time.Since(tStart))
			cont = false
			return
		}

		switch b {
		case ';':
			nameAsInt = hashBytes(byt)
			nameMode = false
			val = 0
			mult = 1
			continue
		case '\n':
			resultsMap[nameAsInt] = newResult(resultsMap[nameAsInt], val)
			if resultsMap[nameAsInt].count == 1 {
				namesMap[nameAsInt] = string(byt[:sepIdx])
			}

			nameMode = true
			byt = byt[:0]
			continue
		}

		switch nameMode {
		case true:
			byt = append(byt, b)
			continue
		case false:
			if b == '-' {
				mult = -1
				b, done = scanner.ReadAByte()
				if done {
					cont = false
					return
				}
			}
			val = int64(b)
			b, done = scanner.ReadAByte()
			if done {
				cont = false
				return
			}
			if b != '.' {
				// not a point so it's a XX.X number
				val = 100*val + 10*int64(b)
				_, done = scanner.ReadAByte()
				if done {
					cont = false
					return
				}
				b, done = scanner.ReadAByte()
				if done {
					cont = false
					return
				}
				val += int64(b)
			} else {
				// we got a point, so it's X.X number
				b, done = scanner.ReadAByte()
				if done {
					cont = false
					return
				}
				val = 10*val + int64(b)
			}
			val = val * mult
			continue
		}
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
