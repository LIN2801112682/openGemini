package tokenRegexQuery

/*
	replace the ith character of the string
*/
func Replacei(old string, pos int, new uint8) string {
	return old[:pos] + string(new) + old[pos+1:]
}

func reverseString(s string) string {
	runes := []rune(s)
	for from, to := 0, len(runes)-1; from < to; from, to = from+1, to-1 {
		runes[from], runes[to] = runes[to], runes[from]
	}
	return string(runes)
}
