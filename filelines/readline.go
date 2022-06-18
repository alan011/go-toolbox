package filelines

import (
	"bufio"
	"os"
)

type FileIterator struct {
	scanner *bufio.Scanner
	file    *os.File
	IsEnd   bool
}

func (iter *FileIterator) ReadLine() string {
	iter.IsEnd = !iter.scanner.Scan()
	line := iter.scanner.Text()
	return line
}

func (iter *FileIterator) Close() {
	iter.file.Close()
}

func Open(filePath string) (*FileIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)

	iter := FileIterator{scanner: scanner, file: file}

	return &iter, nil
}
