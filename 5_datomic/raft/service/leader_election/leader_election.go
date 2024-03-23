package leaderelection

import (
	"encoding/json"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/savemaker/raft/service"
)

func getTimeNowPlusRandomSeconds() time.Time {
	return time.Now().Add(time.Second + time.Duration(rand.Intn(10)))
}

func NewLeaderElectionSerivce(
	node *maelstrom.Node,
	logService *service.LogService,
	stateService *StateService,
	termService *TermService,
	voteService *VoteService,
) *LeaderElectionService {
	electionService := LeaderElectionService{
		node:             node,
		stateService:     stateService,
		electionDeadline: getTimeNowPlusRandomSeconds(),
		electionTimeout:  time.Second * time.Duration(1),
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
	logService       *service.LogService
	voteService      *VoteService
}

func (electionService *LeaderElectionService) scheduleBecomeCandidateJob() {
	stateService := electionService.stateService
	for {
		if stateService.GetCurrentState() == LEADER {
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
	service.electionDeadline = getTimeNowPlusRandomSeconds()
}

func (electionService *LeaderElectionService) becomeCandidate() {
	stateService := electionService.stateService
	termService := electionService.termServcie

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
	request["id"] = service.node.ID()
	request["last_log_index"] = service.logService.Size()
	request["last_log_term"] = service.logService.GetLastEntry().Term

	for _, nID := range service.node.NodeIDs() {
		if nID != service.node.ID() {
			service.node.RPC(nID, request, service.requestVoteCallback)
		}
	}
}

func (service *LeaderElectionService) becomeFollower() {
	stateService := service.stateService
	stateService.changeState(FOLLOWER)
	service.postponeDeadline()
}

func (service *LeaderElectionService) maybeStepDown(term Term) {
	termService := service.termServcie
	if termService.TermOutdated(term) {
		service.becomeFollower()
	}
}

func (service *LeaderElectionService) requestVoteCallback(msg maelstrom.Message) error {
	response := make(map[string]any)
	json.Unmarshal(msg.Body, &response)

	term := response["term"].(Term)
	voteGranted := response["voteGranted"].(bool)
	src := response["src"].(string)

	service.maybeStepDown(term)

	currentTerm := service.termServcie.GetCurentTerm()
	currentState := service.stateService.GetCurrentState()

	if currentState == CANDIDATE && currentTerm == term && voteGranted {
		service.voteService.GetVote(src)
	}

	return nil
}
