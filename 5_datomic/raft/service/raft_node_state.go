package service

import (
	"math/rand"
	"sync"
	"time"
)

const (
	LEADER    State = "Leader"
	CANDIDATE State = "Candidate"
	FOLLOWER  State = "Follower"
)

type State string

type Term int

type RaftStateService interface {
	GetCurrentState() State
	ChangeState(newState State)
}

func getTimeNowPlusRandomSeconds() time.Time {
	return time.Now().Add(time.Second + time.Duration(rand.Intn(10)))
}

func NewRaftStateService() *RaftStateServiceImpl {
	stateService := RaftStateServiceImpl{
		currentState:     FOLLOWER,
		stateLock:        sync.Mutex{},
		electionDeadline: getTimeNowPlusRandomSeconds(),
		electionTimeout:  time.Second * time.Duration(1),
		term:             0,
		termLock:         sync.Mutex{},
	}
	go stateService.scheduleBecomeCandidateJob()
	return &stateService
}

type RaftStateServiceImpl struct {
	currentState     State
	stateLock        sync.Mutex
	electionDeadline time.Time
	electionTimeout  time.Duration
	term             Term
	termLock         sync.Mutex
}

func (service *RaftStateServiceImpl) GetCurrentState() State {
	service.stateLock.Lock()
	defer service.stateLock.Unlock()
	return service.currentState
}

func (service *RaftStateServiceImpl) ChangeState(newState State) {
	service.stateLock.Lock()
	defer service.stateLock.Unlock()
	service.currentState = newState
}

func (service *RaftStateServiceImpl) scheduleBecomeCandidateJob() {
	for {
		if service.GetCurrentState() == LEADER {
			service.postponeDeadline()
		} else {
			if time.Now().After(service.electionDeadline) {
				service.becomeCandidate()
			}
		}
		time.Sleep(service.electionTimeout)
	}
}

func (service *RaftStateServiceImpl) postponeDeadline() {
	service.electionDeadline = getTimeNowPlusRandomSeconds()
}

func (service *RaftStateServiceImpl) becomeCandidate() {
	service.ChangeState(CANDIDATE)
	service.postponeDeadline()
	service.incrementTerm()
}

func (service *RaftStateServiceImpl) incrementTerm() {
	service.termLock.Lock()
	defer service.termLock.Unlock()
	service.term += 1
}
