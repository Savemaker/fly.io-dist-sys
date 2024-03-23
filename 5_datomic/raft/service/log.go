package service

import "sync"

type LogEntry struct {
	Term int
	Op   []any
}

type LogService struct {
	Entries []LogEntry
	lock    sync.Mutex
}

func NewLogService() *LogService {
	return &LogService{
		Entries: []LogEntry{{Term: 0, Op: nil}},
		lock:    sync.Mutex{},
	}
}

func (log *LogService) GetByIndex(index int) LogEntry {
	log.lock.Lock()
	defer log.lock.Unlock()
	return log.Entries[index-1]
}

func (log *LogService) AppendEntries(entries []LogEntry) {
	log.lock.Lock()
	defer log.lock.Unlock()
	log.Entries = append(log.Entries, entries...)
}

func (log *LogService) GetLastEntry() LogEntry {
	log.lock.Lock()
	defer log.lock.Unlock()
	return log.Entries[len(log.Entries)-1]
}

func (log *LogService) Size() int {
	log.lock.Lock()
	defer log.lock.Unlock()
	return len(log.Entries)
}
