package leaderelection

import (
	"math"
	"sync"
)

type VoteService struct {
	votes    map[string]bool
	lock     *sync.Mutex
	votedFor *string
}

func NewVoteService() *VoteService {
	return &VoteService{
		votes:    make(map[string]bool),
		lock:     &sync.Mutex{},
		votedFor: nil,
	}
}

func (voteService *VoteService) CountVote(src string) {
	voteService.lock.Lock()
	defer voteService.lock.Unlock()
	voteService.votes[src] = true
}

func (voteService *VoteService) SetVotedFor(node *string) {
	voteService.lock.Lock()
	defer voteService.lock.Unlock()
	voteService.votedFor = node
}

func (voteService *VoteService) GetVotedFor() *string {
	return voteService.votedFor
}

func (voteService *VoteService) HasMajority(nodes int) bool {
	return int(math.Floor((float64(nodes) / 2))) <= len(voteService.votes)
}
