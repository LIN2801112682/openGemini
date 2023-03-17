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

func And(m1 map[SeriesId]struct{}, s1 []SeriesId, m2 map[SeriesId]struct{}, s2 []SeriesId) map[SeriesId]struct{} {
	result := make(map[SeriesId]struct{})
	if len(m1) > 0 && len(m2) > 0 {
		for key, _ := range m2 {
			if _, ok := m1[key]; ok {
				result[key] = struct{}{}
			}
		}
		return result
	} else if len(s1) > 0 && len(s2) > 0 {
		temp1 := make(map[SeriesId]struct{})
		temp2 := make(map[SeriesId]struct{})
		for _, val1 := range s1 {
			temp1[val1] = struct{}{}
		}
		for _, val2 := range s2 {
			temp2[val2] = struct{}{}
		}
		for key, _ := range temp2 {
			if _, ok := temp1[key]; ok {
				result[key] = struct{}{}
			}
		}
		return result
	} else if len(m1) > 0 && len(s2) > 0 {
		temp2 := make(map[SeriesId]struct{})
		for _, val2 := range s2 {
			temp2[val2] = struct{}{}
		}
		for key, _ := range temp2 {
			if _, ok := m1[key]; ok {
				result[key] = struct{}{}
			}
		}
		return result
	} else if len(m2) > 0 && len(s1) > 0 {
		temp1 := make(map[SeriesId]struct{})
		for _, val1 := range s1 {
			temp1[val1] = struct{}{}
		}
		for key, _ := range temp1 {
			if _, ok := m2[key]; ok {
				result[key] = struct{}{}
			}
		}
		return result
	}
	return result
}

func Or(m1 map[SeriesId]struct{}, s1 []SeriesId, m2 map[SeriesId]struct{}, s2 []SeriesId) map[SeriesId]struct{} {
	result := make(map[SeriesId]struct{})
	if len(m1) > 0 && len(m2) > 0 {
		for key, _ := range m1 {
			result[key] = struct{}{}
		}
		for key, _ := range m2 {
			if _, ok := result[key]; !ok {
				result[key] = struct{}{}
			}
		}
		return result
	} else if len(s1) > 0 && len(s2) > 0 {
		for _, v1 := range s1 {
			if _, ok := result[v1]; !ok {
				result[v1] = struct{}{}
			}
		}
		for _, v2 := range s2 {
			if _, ok := result[v2]; !ok {
				result[v2] = struct{}{}
			}
		}
		return result
	} else if len(m1) > 0 && len(s2) > 0 {
		for key, _ := range m1 {
			result[key] = struct{}{}
		}
		for _, v2 := range s2 {
			if _, ok := result[v2]; !ok {
				result[v2] = struct{}{}
			}
		}
		return result
	} else if len(m2) > 0 && len(s1) > 0 {
		for key, _ := range m2 {
			result[key] = struct{}{}
		}
		for _, v1 := range s1 {
			if _, ok := result[v1]; !ok {
				result[v1] = struct{}{}
			}
		}
		return result
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
