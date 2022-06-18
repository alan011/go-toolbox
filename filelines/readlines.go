package filelines

import (
	"bufio"
	"os"
)

func ReadLines(filePath string) ([]string, error) {
	// file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	res := make([]string, 0)
	for scanner.Scan() {
		line := scanner.Text()
		res = append(res, line)
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

