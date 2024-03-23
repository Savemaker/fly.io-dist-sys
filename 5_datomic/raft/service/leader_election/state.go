package leaderelection

import "sync"

type State string

const (
	FOLLOWER  State = "FOLLOWER"
	CANDIDATE State = "CANDIDATE"
	LEADER    State = "LEADER"
)

type StateService struct {
	lock         *sync.Mutex
	currentState State
}

func NewStateService() *StateService {
	return &StateService{
		lock:         &sync.Mutex{},
		currentState: FOLLOWER,
	}
}

func (state *StateService) GetCurrentState() State {
	state.lock.Lock()
	defer state.lock.Unlock()
	return state.currentState
}

func (state *StateService) changeState(newState State) {
	state.lock.Lock()
	defer state.lock.Unlock()
	state.currentState = newState
}
