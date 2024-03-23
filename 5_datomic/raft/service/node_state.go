package service

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	LEADER    State = "Leader"
	CANDIDATE State = "Candidate"
	FOLLOWER  State = "Follower"
)

type State string

type Term int

type NodeState interface {
	GetCurrentState() State
	ChangeState(newState State)
}

func getTimeNowPlusRandomSeconds() time.Time {
	return time.Now().Add(time.Second + time.Duration(rand.Intn(10)))
}

func NewRaftNodeState(node *maelstrom.Node, log *Log) *RaftNodeState {
	stateService := RaftNodeState{
		node:             node,
		currentState:     FOLLOWER,
		stateLock:        sync.Mutex{},
		electionDeadline: getTimeNowPlusRandomSeconds(),
		electionTimeout:  time.Second * time.Duration(1),
		term:             0,
		termLock:         sync.Mutex{},
		log:              log,
		votes:            make(map[string]bool),
		votesLock:        sync.Mutex{},
	}
	go stateService.scheduleBecomeCandidateJob()
	return &stateService
}

type RaftNodeState struct {
	node             *maelstrom.Node
	currentState     State
	stateLock        sync.Mutex
	electionDeadline time.Time
	electionTimeout  time.Duration
	term             Term
	termLock         sync.Mutex
	log              *Log
	votes            map[string]bool
	votesLock        sync.Mutex
}

func (service *RaftNodeState) GetCurrentState() State {
	service.stateLock.Lock()
	defer service.stateLock.Unlock()
	return service.currentState
}

func (service *RaftNodeState) ChangeState(newState State) {
	service.stateLock.Lock()
	defer service.stateLock.Unlock()
	service.currentState = newState
}

func (service *RaftNodeState) scheduleBecomeCandidateJob() {
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

func (service *RaftNodeState) postponeDeadline() {
	service.electionDeadline = getTimeNowPlusRandomSeconds()
}

func (service *RaftNodeState) becomeCandidate() {
	service.ChangeState(CANDIDATE)
	service.postponeDeadline()
	service.incrementTerm()
	service.requestVotes()
}

func (service *RaftNodeState) requestVotes() {
	request := make(map[string]any)

	request["type"] = "request_votes"
	request["term"] = service.term
	request["id"] = service.node.ID()
	request["last_log_index"] = service.log.Size()
	request["last_log_term"] = service.log.GetLastEntry().Term

	for _, nID := range service.node.NodeIDs() {
		if nID != service.node.ID() {
			service.node.RPC(nID, request, service.requestVoteCallback)
		}
	}
}

func (service *RaftNodeState) incrementTerm() {
	service.termLock.Lock()
	defer service.termLock.Unlock()
	service.term += 1
}

func (service *RaftNodeState) becomeFollower() {
	service.termLock.Lock()
	defer service.termLock.Unlock()
	service.ChangeState(FOLLOWER)
	service.postponeDeadline()
}

func (service *RaftNodeState) maybeStepDown(term Term) {
	service.termLock.Lock()
	defer service.termLock.Unlock()

	if service.term < term {
		service.becomeFollower()
	}
}

func (service *RaftNodeState) requestVoteCallback(msg maelstrom.Message) error {
	response := make(map[string]any)
	json.Unmarshal(msg.Body, &response)

	term := response["term"].(Term)
	voteGranted := response["voteGranted"].(bool)
	src := response["src"].(string)

	service.maybeStepDown(term)

	service.termLock.Lock()
	defer service.termLock.Unlock()

	service.stateLock.Lock()
	defer service.stateLock.Unlock()

	if service.currentState == CANDIDATE && service.term == term && voteGranted {
		service.votesLock.Lock()
		defer service.votesLock.Unlock()

		service.votes[src] = true
	}

	return nil
}
