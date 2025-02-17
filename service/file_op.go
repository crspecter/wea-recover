package service

import (
	"os"
	"sort"
	"strings"
)

var FILE_NAME = "data.xtream"

func MakeDir(path string) error {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func DeletePath(path string) error {
	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}

func CreateAndTruncate(fileNameAndPath string) (*os.File, error) {
	file, err := os.Create(fileNameAndPath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func DeleteFile(path string, file_name string) error {
	if path[len(path)-1] != '/' {
		path += "/"
	}

	filePath := path + file_name
	_, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	err = os.Remove(filePath)
	if err != nil {
		return err
	}
	return nil
}

func IsFile(path string) bool {
	file, err := os.Stat(path)
	if err != nil {
		return false
	}
	if file.IsDir() {
		return false
	}

	return true
}

type DirEntrys []os.DirEntry

func (f DirEntrys) Len() int {
	return len(f)
}

func (f DirEntrys) Less(i, j int) bool {
	ret := strings.Compare(f[i].Name(), f[j].Name())
	if ret == 0 {
		return true
	} else if ret < 0 {
		return true
	} else {
		return false
	}
}

func (f DirEntrys) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func ListFile(path string) ([]os.DirEntry, error) {
	var files DirEntrys
	dir, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, f := range dir {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if strings.HasPrefix(name, "mysql-bin.") || strings.HasPrefix(name, "binlog.0") {
			files = append(files, f)
		}
	}
	sort.Sort(files)
	return files, nil

}
