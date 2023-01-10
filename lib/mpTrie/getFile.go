package mpTrie

import (
	"fmt"
	"os"
)

func ReadByteFormFilePtr(file *os.File,start,end int64,mode int) []byte{
	size := end-start
	resbyte := make([]byte,size)
	_, err := file.Seek(start, mode)
	if err != nil {
		return nil
	}
	_, err = file.Read(resbyte)
	if err != nil {
		return nil
	}
	return resbyte
}
func ReadOffByteFormFilePtr(file *os.File, start, offset int64, mode int) ([]byte, int64) {
	resbyte := make([]byte,offset)
	_, err := file.Seek(start, mode)
	if err != nil {
		return nil, 0
	}
	_, err = file.Read(resbyte)
	if err != nil {
		return nil, 0
	}
	return resbyte,start+offset
}
func GetFilePiont(filename string) (*os.File,int64) {
	file,err := os.OpenFile(filename,os.O_RDONLY,os.ModePerm)
	if err!=nil{
		fmt.Println(err)
		return nil,0
	}
	stat, err := file.Stat()
	size := stat.Size()
	if err != nil {
		return nil, 0
	}
	return file,size
}
func decodeSerializeObj(buffer []byte) (string, *SerializeObj) {
	stdlen := DEFAULT_SIZE
	freq,_ := BytesToInt(buffer[:FREQ_SIZE],false)
	buffer = buffer[FREQ_SIZE:]
	objaddrlen, _ := BytesToInt(buffer[:stdlen], false)
	buffer = buffer[stdlen:]
	var addrListEntry = new(AddrListEntry)
	if objaddrlen != 0 {
		size, _ := BytesToInt(buffer[:stdlen], false)
		off, _ := BytesToInt(buffer[stdlen:2*stdlen], false)
		addrListEntry.SetSize(uint64(size))
		addrListEntry.SetBlockoffset(uint64(off))
		buffer = buffer[2*stdlen:]
	}
	objinvtdlen, _ := BytesToInt(buffer[:stdlen], false)
	buffer = buffer[stdlen:]
	var invtdListEntry = new(InvertedListEntry)
	if objinvtdlen != 0 {
		size, _ := BytesToInt(buffer[:stdlen], false)
		mintime, _ := BytesToInt(buffer[stdlen:2*stdlen], false)
		maxtime, _ := BytesToInt(buffer[2*stdlen:3*stdlen], false)
		off, _ := BytesToInt(buffer[3*stdlen:4*stdlen], false)
		invtdListEntry.SetSize(uint64(size))
		invtdListEntry.SetMinTime(int64(mintime))
		invtdListEntry.SetMaxTime(int64(maxtime))
		invtdListEntry.SetBlockoffset(uint64(off))
		buffer = buffer[4*stdlen:]
	}
	data := string(buffer)
	obj := NewSerializeObj(data, uint32(freq),uint64(objaddrlen), addrListEntry, uint64(objinvtdlen), invtdListEntry)
	obj.UpdateSeializeObjSize()
	return data, obj
}

