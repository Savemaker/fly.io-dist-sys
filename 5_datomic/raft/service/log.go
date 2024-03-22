package service

import "sync"

type LogEntry struct {
	Term int
	Op   []any
}

type Log struct {
	Entries []LogEntry
	lock    sync.Mutex
}

func NewLog() *Log {
	return &Log{
		Entries: []LogEntry{{Term: 0, Op: nil}},
		lock:    sync.Mutex{},
	}
}

func (log *Log) GetByIndex(index int) LogEntry {
	log.lock.Lock()
	defer log.lock.Unlock()
	return log.Entries[index-1]
}

func (log *Log) AppendEntries(entries []LogEntry) {
	log.lock.Lock()
	defer log.lock.Unlock()
	log.Entries = append(log.Entries, entries...)
}

func (log *Log) GetLastEntry() LogEntry {
	log.lock.Lock()
	defer log.lock.Unlock()
	return log.Entries[len(log.Entries)-1]
}
