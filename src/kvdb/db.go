package kvdb

import ("sort";"sync")

type DB struct { mu sync.RWMutex; storage *storage; values map[string][]byte; closed bool }

func Open(path string,maxBytes int64)(*DB,error){s,recovered,err:=openStorage(path,maxBytes);if err!=nil{return nil,err};d:=&DB{storage:s,values:map[string][]byte{}};for k,r:=range recovered{if r.Type==Put{d.values[k]=append([]byte(nil),r.Value...)}};return d,nil}
func (db *DB) Put(k,v []byte)error{return db.Batch([]Operation{{Type:Put,Key:k,Value:v}})}
func (db *DB) Delete(k []byte)error{return db.Batch([]Operation{{Type:Delete,Key:k}})}
func (db *DB) Get(k []byte)([]byte,error){db.mu.RLock();defer db.mu.RUnlock();if db.closed{return nil,ErrClosed};v,ok:=db.values[string(k)];if !ok{return nil,ErrNotFound};return append([]byte(nil),v...),nil}
func (db *DB) Batch(ops []Operation)error{if len(ops)>10000{return &limitError{"batch exceeds 10000 operations"}};db.mu.Lock();defer db.mu.Unlock();if db.closed{return ErrClosed};for _,op:=range ops{if len(op.Key)>1024{return &limitError{"key exceeds 1024 bytes"}};if op.Type==Put&&len(op.Value)>1<<20{return &limitError{"value exceeds 1 MB"}};if op.Type!=Put&&op.Type!=Delete{return &limitError{"unknown operation"}}};if err:=db.storage.commit(ops);err!=nil{return err};for _,op:=range ops{if op.Type==Delete{delete(db.values,string(op.Key))}else{db.values[string(op.Key)]=append([]byte(nil),op.Value...)}};return nil}
func (db *DB) Keys()map[string][]byte{db.mu.RLock();defer db.mu.RUnlock();out:=map[string][]byte{};for k,v:=range db.values{out[k]=append([]byte(nil),v...)};return out}
func (db *DB) Check()error{db.mu.RLock();defer db.mu.RUnlock();if db.closed{return ErrClosed};return db.storage.check()}
func (db *DB) Close()error{db.mu.Lock();defer db.mu.Unlock();if db.closed{return nil};db.closed=true;return db.storage.close()}
type limitError struct{s string};func(e *limitError)Error()string{return e.s}
func SortedKeys(m map[string][]byte)[]string{r:=make([]string,0,len(m));for k:=range m{r=append(r,k)};sort.Strings(r);return r}
