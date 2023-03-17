package utils

type TimePoint struct {
	TimeStamp int64
	Pos       *[]uint16
}

func NewTimePoint(timeStamp int64, pos *[]uint16) TimePoint {
	return TimePoint{TimeStamp: timeStamp, Pos: pos}
}
