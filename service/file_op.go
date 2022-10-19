package service

import (
	"os"
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
