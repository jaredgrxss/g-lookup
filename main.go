package main

import (
	"fmt"
	"math/rand"
	"os"
)

func main() {
	fmt.Println("Hello World")
	
}

// bad, serious limitations
func Save1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync()
}

// better, atomic in nature w.r.t readers, but not durable (i.e power outage)
func Save2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data)
	if err != nil {
		return err 
	}
	err = fp.Sync()
	if err != nil {
		return err 
	}
	return os.Rename(tmp, path) // this is atomic in nature by pointers to file names
}