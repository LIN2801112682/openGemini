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

package record_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func checkMaxValue(rec *record.Record, iv int64, iIdx int, fv float64, fIdx int, bv bool, bIdx int) bool {
	fields := rec.Schemas()
	rows := rec.RowNums()
	for i := 0; i < rec.ColNums()-1; i++ {
		col := rec.Column(i)
		if fields[i].Type == influx.Field_Type_Int {
			maxIntValue, maxIntIndex := col.MaxIntegerValue(col.IntegerValues(), 0, rows)
			if !(maxIntValue == iv && maxIntIndex == iIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Float {
			maxFloatValue, maxFloatIndex := col.MaxFloatValue(col.FloatValues(), 0, rows)
			if !(maxFloatValue == fv && maxFloatIndex == fIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Boolean {
			maxBoolValue, maxBoolIndex := col.MaxBooleanValue(col.BooleanValues(), 0, rows)
			if !(maxBoolValue == bv && maxBoolIndex == bIdx) {
				return false
			}
		}
	}
	return true
}

func checkMinValue(rec *record.Record, iv int64, iIdx int, fv float64, fIdx int, bv bool, bIdx int) bool {
	fields := rec.Schemas()
	rows := rec.RowNums()
	for i := 0; i < rec.ColNums()-1; i++ {
		col := rec.Column(i)
		if fields[i].Type == influx.Field_Type_Int {
			minIntValue, minIntIndex := col.MinIntegerValue(col.IntegerValues(), 0, rows)
			if !(minIntValue == iv && minIntIndex == iIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Float {
			minFloatValue, minFloatIndex := col.MinFloatValue(col.FloatValues(), 0, rows)
			if !(minFloatValue == fv && minFloatIndex == fIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Boolean {
			minBoolValue, minBoolIndex := col.MinBooleanValue(col.BooleanValues(), 0, rows)
			if !(minBoolValue == bv && minBoolIndex == bIdx) {
				return false
			}
		}
	}
	return true
}

func checkFirstValue(rec *record.Record, iv int64, iIdx int, fv float64, fIdx int, bv bool, bIdx int, sv string, sIdx int) bool {
	fields := rec.Schemas()
	rows := rec.RowNums()
	for i := 0; i < rec.ColNums()-1; i++ {
		col := rec.Column(i)
		if fields[i].Type == influx.Field_Type_Int {
			firstIntValue, firstIntIndex := col.FirstIntegerValue(col.IntegerValues(), 0, rows)
			if !(firstIntValue == iv && firstIntIndex == iIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Float {
			firstFloatValue, firstFloatIndex := col.FirstFloatValue(col.FloatValues(), 0, rows)
			if !(firstFloatValue == fv && firstFloatIndex == fIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Boolean {
			firstBoolValue, firstBoolIndex := col.FirstBooleanValue(col.BooleanValues(), 0, rows)
			if !(firstBoolValue == bv && firstBoolIndex == bIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_String {
			var strs []string
			strs = col.StringValues(strs)
			firstStringValue, firstStringIndex := col.FirstStringValue(strs, 0, rows)
			if !(firstStringValue == sv && firstStringIndex == sIdx) {
				return false
			}
		}
	}
	return true
}

func checkLastValue(rec *record.Record, iv int64, iIdx int, fv float64, fIdx int, bv bool, bIdx int, sv string, sIdx int) bool {
	fields := rec.Schemas()
	rows := rec.RowNums()
	for i := 0; i < rec.ColNums()-1; i++ {
		col := rec.Column(i)
		if fields[i].Type == influx.Field_Type_Int {
			lastIntValue, lastIntIndex := col.LastIntegerValue(col.IntegerValues(), 0, rows)
			if !(lastIntValue == iv && lastIntIndex == iIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Float {
			lastFloatValue, lastFloatIndex := col.LastFloatValue(col.FloatValues(), 0, rows)
			if !(lastFloatValue == fv && lastFloatIndex == fIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_Boolean {
			lastBoolValue, lastBoolIndex := col.LastBooleanValue(col.BooleanValues(), 0, rows)
			if !(lastBoolValue == bv && lastBoolIndex == bIdx) {
				return false
			}
		} else if fields[i].Type == influx.Field_Type_String {
			var strs []string
			strs = col.StringValues(strs)
			lastStringValue, lastStringIndex := col.LastStringValue(strs, 0, rows)
			if !(lastStringValue == sv && lastStringIndex == sIdx) {
				return false
			}
		}
	}
	return true
}

func TestMaxValue(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 1, 1}, []int64{700, 0, 600, 800},
		[]int{1, 0, 1, 0}, []float64{2.2, 0, 5.3, 0},
		[]int{1, 0, 1, 1}, []string{"test", "hi", "world", "ok"},
		[]int{1, 0, 1, 0}, []bool{true, false, false, true},
		[]int64{1, 2, 3, 4})

	if !checkMaxValue(rec, 800, 3, 5.3, 2, true, 0) {
		t.Fatal("check max value failed failed")
	}
}

func TestMinValue(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 1, 1}, []int64{700, 0, 600, 800},
		[]int{1, 0, 1, 0}, []float64{2.2, 0, 5.3, 0},
		[]int{1, 0, 1, 1}, []string{"test", "hi", "world", "ok"},
		[]int{1, 0, 1, 0}, []bool{true, false, false, true},
		[]int64{1, 2, 3, 4})

	if !checkMinValue(rec, 600, 2, 2.2, 0, false, 2) {
		t.Fatal("check failed")
	}
}

func TestFirstValue(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 1, 1}, []int64{700, 0, 600, 800},
		[]int{1, 0, 1, 0}, []float64{2.2, 0, 5.3, 0},
		[]int{1, 0, 1, 1}, []string{"test", "hi", "world", "ok"},
		[]int{1, 0, 1, 0}, []bool{true, false, false, true},
		[]int64{1, 2, 3, 4})

	if !checkFirstValue(rec, 700, 0, 2.2, 0, true, 0, "test", 0) {
		t.Fatal("check failed")
	}
}

func TestLastValue(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 1, 1}, []int64{700, 0, 600, 800},
		[]int{1, 0, 1, 0}, []float64{2.2, 0, 5.3, 0},
		[]int{1, 0, 1, 1}, []string{"test", "hi", "world", "ok"},
		[]int{1, 0, 1, 0}, []bool{true, false, false, true},
		[]int64{1, 2, 3, 4})

	if !checkLastValue(rec, 800, 3, 5.3, 2, false, 2, "ok", 3) {
		t.Fatal("check failed")
	}
}

func TestAggValues(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	recWithEmpty := genRowRec(schema,
		[]int{1, 1, 0, 0, 1, 1, 1, 1}, []int64{700, 700, 0, 0, 600, 600, 800, 800},
		[]int{1, 1, 0, 0, 1, 1, 1, 1}, []float64{2.2, 2.2, 0, 0, 5.3, 5.3, 1.1, 1.1},
		[]int{1, 1, 0, 0, 1, 1, 1, 1}, []string{"test", "test", "hi", "hi", "world", "world", "ok", "ok"},
		[]int{1, 1, 0, 0, 1, 1, 0, 0}, []bool{true, false, false, false, false, true, false, false},
		[]int64{1, 2, 3, 4, 5, 6, 7, 8})

	recNoNil := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1, 1}, []int64{700, 700, 650, 650, 600, 600, 800, 800},
		[]int{1, 1, 1, 1, 1, 1, 1, 1}, []float64{2.2, 2.2, 3.3, 3.3, 5.3, 5.3, 1.1, 1.1},
		[]int{1, 1, 1, 1, 1, 1, 1, 1}, []string{"test", "test", "hi", "hi", "world", "world", "ok", "ok"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1}, []bool{true, false, false, false, false, true, false, false},
		[]int64{1, 2, 3, 4, 5, 6, 7, 8})
	//check values:
	for i := 0; i < len(schema)-1; i++ {
		switch schema[i].Type {
		case influx.Field_Type_Float:
			values1 := recWithEmpty.ColVals[i].FloatValues()
			values2 := recNoNil.ColVals[i].FloatValues()
			//check max values
			VMax1, rowsMax1 := recWithEmpty.ColVals[i].MaxFloatValues(values1, 0, recWithEmpty.RowNums())
			if VMax1 != 5.3 || len(rowsMax1) != 2 || rowsMax1[0] != 4 || rowsMax1[1] != 5 {
				t.Fatal("float max values wrong!")
			}
			vMin1, rowsMin1 := recWithEmpty.ColVals[i].MinFloatValues(values1, 0, recWithEmpty.RowNums())
			if vMin1 != 1.1 || len(rowsMin1) != 2 || rowsMin1[0] != 6 || rowsMin1[1] != 7 {
				t.Fatal("float min values wrong!")
			}
			VMax2, rowsMax2 := recNoNil.ColVals[i].MaxFloatValues(values2, 0, recNoNil.RowNums())
			if VMax2 != 5.3 || len(rowsMax2) != 2 || rowsMax2[0] != 4 || rowsMax2[1] != 5 {
				t.Fatal("float max values wrong!")
			}
			vMin2, rowsMin2 := recNoNil.ColVals[i].MinFloatValues(values2, 0, recNoNil.RowNums())
			if vMin2 != 1.1 || len(rowsMin2) != 2 || rowsMin2[0] != 6 || rowsMin2[1] != 7 {
				t.Fatal("float min values wrong!")
			}
		case influx.Field_Type_Int:
			values1 := recWithEmpty.ColVals[i].IntegerValues()
			values2 := recNoNil.ColVals[i].IntegerValues()
			//check max values
			VMax1, rowsMax1 := recWithEmpty.ColVals[i].MaxIntegerValues(values1, 0, recWithEmpty.RowNums())
			if VMax1 != 800 || len(rowsMax1) != 2 || rowsMax1[0] != 6 || rowsMax1[1] != 7 {
				t.Fatal("integer max values wrong!")
			}
			vMin1, rowsMin1 := recWithEmpty.ColVals[i].MinIntegerValues(values1, 0, recWithEmpty.RowNums())
			if vMin1 != 600 || len(rowsMin1) != 2 || rowsMin1[0] != 4 || rowsMin1[1] != 5 {
				t.Fatal("integer min values wrong!")
			}
			VMax2, rowsMax2 := recNoNil.ColVals[i].MaxIntegerValues(values2, 0, recNoNil.RowNums())
			if VMax2 != 800 || len(rowsMax2) != 2 || rowsMax2[0] != 6 || rowsMax2[1] != 7 {
				t.Fatal("integer max values wrong!")
			}
			vMin2, rowsMin2 := recNoNil.ColVals[i].MinIntegerValues(values2, 0, recNoNil.RowNums())
			if vMin2 != 600 || len(rowsMin2) != 2 || rowsMin2[0] != 4 || rowsMin2[1] != 5 {
				t.Fatal("integer min values wrong!")
			}
		case influx.Field_Type_Boolean:
			values1 := recWithEmpty.ColVals[i].BooleanValues()
			values2 := recNoNil.ColVals[i].BooleanValues()
			//check max values
			VMax1, rowsMax1 := recWithEmpty.ColVals[i].MaxBooleanValues(values1, 0, recWithEmpty.RowNums())
			if !VMax1 || len(rowsMax1) != 2 || rowsMax1[0] != 0 || rowsMax1[1] != 5 {
				t.Fatal("boolean max values wrong!")
			}
			vMin1, rowsMin1 := recWithEmpty.ColVals[i].MinBooleanValues(values1, 0, recWithEmpty.RowNums())
			if vMin1 || len(rowsMin1) != 2 || rowsMin1[0] != 1 || rowsMin1[1] != 4 {
				t.Fatal("boolean min values wrong!")
			}
			VMax2, rowsMax2 := recNoNil.ColVals[i].MaxBooleanValues(values2, 0, recNoNil.RowNums())
			if !VMax2 || len(rowsMax2) != 2 || rowsMax2[0] != 0 || rowsMax2[1] != 5 {
				t.Fatal("boolean max values wrong!")
			}
			vMin2, rowsMin2 := recNoNil.ColVals[i].MinBooleanValues(values2, 0, recNoNil.RowNums())
			if vMin2 || len(rowsMin2) != 6 || rowsMin2[0] != 1 || rowsMin2[1] != 2 || rowsMin2[2] != 3 || rowsMin2[3] != 4 ||
				rowsMin2[4] != 6 || rowsMin2[5] != 7 {
				t.Fatal("boolean min values wrong!")
			}
		}
	}
}
