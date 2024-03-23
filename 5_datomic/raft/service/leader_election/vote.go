package leaderelection

import "sync"

type VoteService struct {
	votes map[string]bool
	lock  *sync.Mutex
}

func NewVoteService() *VoteService {
	return &VoteService{
		votes: make(map[string]bool),
		lock:  &sync.Mutex{},
	}
}

func (voteService *VoteService) GetVote(src string) {
	voteService.lock.Lock()
	defer voteService.lock.Unlock()
	voteService.votes[src] = true
}
