/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package utils

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func GetLinesFromFile(filename string) []string {
	var res = make([]string, 0)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	buff := bufio.NewReader(file)
	for {
		data, _, eof := buff.ReadLine()
		if eof == io.EOF {
			break
		}
		str := (string)(data)
		res = append(res, str)
	}
	return res
}

func GetAllFile(pathname string, s []string) ([]string, error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		//fmt.Println("read dir fail:", err)
		return s, err
	}
	for _, fi := range rd {
		if !fi.IsDir() {
			fullName := pathname + fi.Name()
			s = append(s, fullName)
		}
	}
	return s, nil
}

func FileExist(path string) bool {
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}
