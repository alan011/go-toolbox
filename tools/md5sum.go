package tools

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
)

func Md5sum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	md5hash := md5.New()
	if _, err := io.Copy(md5hash, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", md5hash.Sum(nil)), nil
}

// func Md5sum(path string) string {
// 	f, err := os.Open(path)
// 	if err != nil {
// 		panic(err)
// 	}

// 	defer f.Close()

// 	body, err := ioutil.ReadAll(f)
// 	if err != nil {
// 		panic(err)
// 	}

// 	return fmt.Sprintf("%x", md5.Sum(body))
// 	//fmt.Printf("%x\n", md5.Sum(body))
// }
