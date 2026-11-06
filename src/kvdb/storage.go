package kvdb

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var ErrNotFound = errors.New("key not found")
var ErrClosed = errors.New("database is closed")

type OperationType uint8

const (
	Put OperationType = iota + 1
	Delete
)

type Operation struct { Type OperationType; Key, Value []byte }
type RecordLocation struct { Offset int64; Length uint32 }

type diskRecord struct { TxID uint64; Type OperationType; Key, Value []byte; Commit bool }

type storage struct { mu sync.Mutex; f *os.File; maxBytes int64; nextTx uint64 }

func openStorage(path string, maxBytes int64) (*storage, map[string]diskRecord, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600); if err != nil { return nil,nil,err }
	s := &storage{f:f,maxBytes:maxBytes}; committed := map[string]diskRecord{}
	if err := s.recover(committed); err != nil { f.Close(); return nil,nil,err }
	return s, committed, nil
}

func (s *storage) recover(index map[string]diskRecord) error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil { return err }
	pending := map[uint64][]diskRecord{}; var pos int64
	r := bufio.NewReader(s.f)
	for {
		var n uint32; if err := binary.Read(r,binary.LittleEndian,&n); err != nil { if err==io.EOF { break }; return err }
		if n == 0 || n > 64<<20 { return fmt.Errorf("invalid record length %d at %d",n,pos) }
		b:=make([]byte,n); if _,err:=io.ReadFull(r,b);err!=nil{return err}; pos += int64(4+n)
		var rec diskRecord; if err:=json.Unmarshal(b,&rec);err!=nil{return err}; if rec.TxID>s.nextTx{s.nextTx=rec.TxID}
		if rec.Commit { for _,op:=range pending[rec.TxID] { index[string(op.Key)] = op }; delete(pending,rec.TxID) } else { pending[rec.TxID]=append(pending[rec.TxID],rec) }
	}
	_,err:=s.f.Seek(0,io.SeekEnd); return err
}

func (s *storage) appendLocked(rec diskRecord) error {
	b,err:=json.Marshal(rec);if err!=nil{return err}; n:=uint32(len(b)); if s.maxBytes>0 { st,_:=s.f.Stat(); if st.Size()+int64(4+n)>s.maxBytes{return fmt.Errorf("database size limit exceeded")} }
	if err=binary.Write(s.f,binary.LittleEndian,n);err!=nil{return err}; _,err=s.f.Write(b);return err
}

func (s *storage) commit(ops []Operation) error {
	s.mu.Lock(); defer s.mu.Unlock(); s.nextTx++; tx:=s.nextTx
	for _,op:=range ops { if err:=s.appendLocked(diskRecord{TxID:tx,Type:op.Type,Key:append([]byte(nil),op.Key...),Value:append([]byte(nil),op.Value...)});err!=nil{return err} }
	if err:=s.appendLocked(diskRecord{TxID:tx,Commit:true});err!=nil{return err}; return s.f.Sync()
}

func (s *storage) close() error { s.mu.Lock(); defer s.mu.Unlock(); if s.f==nil{return nil}; err:=s.f.Sync(); e:=s.f.Close(); s.f=nil; if err!=nil{return err};return e }
func (s *storage) check() error { s.mu.Lock(); defer s.mu.Unlock(); _,err:=s.f.Seek(0,io.SeekStart); if err!=nil{return err}; _,_,err=openStorageCheck(s.f); return err }
func openStorageCheck(f *os.File)(int64,int64,error){ var pos,records int64; r:=bufio.NewReader(f); for { var n uint32;err:=binary.Read(r,binary.LittleEndian,&n);if err==io.EOF{break};if err!=nil{return pos,records,err};if n==0||n>64<<20{return pos,records,fmt.Errorf("invalid record length at %d",pos)};b:=make([]byte,n);if _,err=io.ReadFull(r,b);err!=nil{return pos,records,err};var rec diskRecord;if err=json.Unmarshal(b,&rec);err!=nil{return pos,records,err};pos+=int64(n)+4;records++ };_,err:=f.Seek(0,io.SeekEnd);return pos,records,err }
