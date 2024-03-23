package leaderelection

import "sync"

type Term int

type TermService struct {
	term Term
	lock *sync.Mutex
}

func NewTermService() *TermService {
	return &TermService{
		term: Term(0),
		lock: &sync.Mutex{},
	}
}

func (service *TermService) incrementTerm() {
	service.lock.Lock()
	defer service.lock.Unlock()
	service.term += 1
}

func (service *TermService) GetCurentTerm() Term {
	service.lock.Lock()
	defer service.lock.Unlock()
	return service.term
}

func (service *TermService) TermOutdated(term Term) bool {
	service.lock.Lock()
	defer service.lock.Unlock()
	return service.term < term
}
