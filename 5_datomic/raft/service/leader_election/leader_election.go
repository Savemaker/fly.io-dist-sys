package leaderelection

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func NewLeaderElectionSerivce(
	node *maelstrom.Node,
	logService *LogService,
	stateService *StateService,
	termService *TermService,
	voteService *VoteService,
) *LeaderElectionService {
	electionService := LeaderElectionService{
		node:             node,
		stateService:     stateService,
		electionDeadline: time.Now().Add(time.Second * time.Duration(rand.Intn(10))),
		electionTimeout:  time.Second * time.Duration(10),
		termServcie:      termService,
		logService:       logService,
		voteService:      voteService,
	}
	go electionService.scheduleBecomeCandidateJob()
	return &electionService
}

type LeaderElectionService struct {
	node             *maelstrom.Node
	stateService     *StateService
	electionDeadline time.Time
	electionTimeout  time.Duration
	termServcie      *TermService
	logService       *LogService
	voteService      *VoteService
}

type VoteGrantedRequest struct {
	CandidateID           string
	CandidateTerm         Term
	CandidateLastLogTerm  Term
	CandidateLastLogIndex int
}

type VoteGrantedResponse struct {
	VoteGranted bool
	Term        Term
}

func (service *LeaderElectionService) VoteGranted(request VoteGrantedRequest) VoteGrantedResponse {
	service.maybeStepDown(request.CandidateTerm)

	currentTerm := service.termServcie.GetCurentTerm()
	votedFor := service.voteService.GetVotedFor()
	lastLogIndex := service.logService.Size()
	latestLogTerm := service.logService.GetLastEntry().Term

	granted := votedFor == nil &&
		currentTerm <= request.CandidateTerm &&
		latestLogTerm <= request.CandidateLastLogTerm &&
		lastLogIndex <= request.CandidateLastLogIndex

	if granted {
		service.voteService.SetVotedFor(&request.CandidateID)
	}

	return VoteGrantedResponse{
		VoteGranted: granted,
		Term:        currentTerm,
	}
}

func (electionService *LeaderElectionService) scheduleBecomeCandidateJob() {
	stateService := electionService.stateService
	for {
		if stateService.GetCurrentState() == LEADER {
			log.Default().Print("leader=" + electionService.node.ID() + " term=" + strconv.Itoa(int(electionService.termServcie.GetCurentTerm())))
			electionService.postponeDeadline()
		} else {
			if time.Now().After(electionService.electionDeadline) {
				electionService.becomeCandidate()
			}
		}
		time.Sleep(electionService.electionTimeout)
	}
}

func (service *LeaderElectionService) postponeDeadline() {
	service.electionDeadline = time.Now().Add(time.Second * time.Duration(rand.Intn(10)))
}

func (electionService *LeaderElectionService) becomeCandidate() {
	stateService := electionService.stateService
	termService := electionService.termServcie
	voteService := electionService.voteService
	id := electionService.node.ID()
	voteService.SetVotedFor(&id)
	stateService.changeState(CANDIDATE)
	electionService.postponeDeadline()
	termService.incrementTerm()
	electionService.requestVotes()
}

func (service *LeaderElectionService) requestVotes() {
	termService := service.termServcie
	request := make(map[string]any)

	request["type"] = "request_votes"
	request["term"] = termService.GetCurentTerm()
	request["lastLogIndex"] = service.logService.Size()
	request["lastLogTerm"] = service.logService.GetLastEntry().Term

	for _, nID := range service.node.NodeIDs() {
		if nID != service.node.ID() {
			service.node.RPC(nID, request, service.requestVoteCallback)
		}
	}
}

func (service *LeaderElectionService) becomeFollower(newTerm Term) {
	stateService := service.stateService
	stateService.changeState(FOLLOWER)
	service.postponeDeadline()
	service.termServcie.advanceTerm(newTerm)
	service.voteService.SetVotedFor(nil)
}

func (service *LeaderElectionService) becomeLeader() {
	stateService := service.stateService
	if stateService.GetCurrentState() == CANDIDATE {
		stateService.changeState(LEADER)
	}
}

func (service *LeaderElectionService) maybeStepDown(term Term) {
	termService := service.termServcie
	if termService.TermOutdated(term) {
		service.becomeFollower(term)
	}
}

func (service *LeaderElectionService) requestVoteCallback(msg maelstrom.Message) error {
	response := make(map[string]any)
	json.Unmarshal(msg.Body, &response)

	term := Term(response["term"].(float64))
	voteGranted := response["voteGranted"].(bool)
	src := msg.Src

	service.maybeStepDown(term)

	currentTerm := service.termServcie.GetCurentTerm()
	currentState := service.stateService.GetCurrentState()

	if currentState == CANDIDATE && currentTerm == term && voteGranted {
		service.voteService.CountVote(src)
		if service.voteService.HasMajority(len(service.node.NodeIDs())) {
			service.becomeLeader()
		}
	}
	return nil
}
