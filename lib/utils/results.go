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

type SeriesId struct {
	Id   uint64
	Time int64
}

func NewSeriesId(id uint64, t int64) SeriesId {
	return SeriesId{
		Id:   id,
		Time: t,
	}
}

func And(s1, s2 map[SeriesId]struct{}) map[SeriesId]struct{} {
	result := make(map[SeriesId]struct{})
	for key, _ := range s2 {
		if _, ok := s1[key]; !ok {
			result[key] = struct{}{}
		}
	}
	return result
}

func Or(s1, s2 map[SeriesId]struct{}) map[SeriesId]struct{} {
	result := make(map[SeriesId]struct{})
	for key, _ := range s1 {
		result[key] = struct{}{}
	}
	for key, _ := range s2 {
		if _, ok := result[key]; !ok {
			result[key] = struct{}{}
		}
	}
	return result
}

func OrMaps(maps ...map[SeriesId]struct{}) map[SeriesId]struct{} {
	result := make(map[SeriesId]struct{})
	for i := 0; i < len(maps); i++ {
		if len(maps[i]) > 0 {
			for key, _ := range maps[i] {
				result[key] = struct{}{}
			}
		}
	}
	return result
}
