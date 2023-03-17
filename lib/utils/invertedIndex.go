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

type Inverted_index map[SeriesId][]uint16

type InvertedIndex map[uint64][]TimePoint

func InvertedIndexChange(invertedIndex Inverted_index) InvertedIndex {
	invertedIndex_new := make(InvertedIndex)
	for sid, posList := range invertedIndex {
		tsid := sid.Id
		timeStamp := sid.Time
		pos := &posList
		timePoint := NewTimePoint(timeStamp, pos)
		if _, ok := invertedIndex_new[tsid]; !ok {
			timePoints := make([]TimePoint, 0)
			timePoints = append(timePoints, timePoint)
			invertedIndex_new[tsid] = timePoints
		} else {
			invertedIndex_new[tsid] = append(invertedIndex_new[tsid], timePoint)
		}
	}
	return invertedIndex_new
}
