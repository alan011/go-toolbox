package tools

import (
	"io/fs"
	"os"
)

func IsFile(path string) bool {
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return false
	}
	mode := fileInfo.Mode()
	return mode.IsRegular()
}

func IsDir(path string) bool {
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return false
	}
	mode := fileInfo.Mode()
	return mode.IsDir()
}

func IsSymLink(path string) bool {
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return false
	}
	mode := fileInfo.Mode()
	return mode&fs.ModeSymlink != 0
}

func IsNamedPipe(path string) bool {
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return false
	}
	mode := fileInfo.Mode()
	return mode&fs.ModeNamedPipe != 0
}
