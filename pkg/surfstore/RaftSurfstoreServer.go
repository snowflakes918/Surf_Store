package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"math"
	"strings"
	"sync"
	"time"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []*chan bool

	lastApplied  int64
	nextIndex    []int64
	matchedIndex []int64

	// Server Info
	peers []string
	id    int64

	// Leader protection
	isLeaderMutex *sync.RWMutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	appendLock     *sync.RWMutex

	UnimplementedRaftSurfstoreServer
	ip string
}

func (s *RaftSurfstore) checkConn(server_ip string, conn_channel chan bool) {
	for {
		conn, err := grpc.Dial(server_ip, grpc.WithInsecure())
		if err != nil {
			continue
		}

		c := NewRaftSurfstoreClient(conn)
		dummy := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  0,
			PrevLogIndex: 0,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err = c.AppendEntries(ctx, dummy)
		conn.Close()
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
		} else {
			conn_channel <- true
			return
		}
	}
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	//s.isCrashedMutex.RUnlock()

	//s.isLeaderMutex.RLock()
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	//s.isLeaderMutex.RUnlock()

	conn_channel := make(chan bool, len(s.peers)-1)
	conn_count := 1
	for idx, ip := range s.peers {
		if s.id != int64(idx) {
			go s.checkConn(ip, conn_channel)
		}
	}

	for {
		success := <-conn_channel
		if success {
			conn_count++
		}

		if conn_count > len(s.peers)/2 {
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}
	}

	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	//s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	//s.isCrashedMutex.RUnlock()

	//s.isLeaderMutex.RLock()
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	//s.isLeaderMutex.RUnlock()

	conn_channel := make(chan bool, len(s.peers)-1)
	conn_count := 1
	for idx, ip := range s.peers {
		if s.id != int64(idx) {
			go s.checkConn(ip, conn_channel)
		}
	}

	for {
		success := <-conn_channel
		if success {
			conn_count++
		}

		if conn_count > len(s.peers)/2 {
			return s.metaStore.GetBlockStoreMap(ctx, hashes)
		}
	}

	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	//s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	//s.isCrashedMutex.RUnlock()

	//s.isLeaderMutex.RLock()
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	//s.isLeaderMutex.RUnlock()

	conn_channel := make(chan bool, len(s.peers)-1)
	conn_count := 1
	for idx, ip := range s.peers {
		if s.id != int64(idx) {
			go s.checkConn(ip, conn_channel)
		}
	}

	for {
		success := <-conn_channel
		if success {
			conn_count++
		}

		if conn_count > len(s.peers)/2 {
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		}
	}

	return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	fmt.Println("update file")
	//s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	//s.isCrashedMutex.RUnlock()

	//s.isLeaderMutex.RLock()
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	//s.isLeaderMutex.RUnlock()

	updateOperation := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	// append entry to our log
	s.appendLock.Lock()
	s.log = append(s.log, &updateOperation)
	s.appendLock.Unlock()

	commitChannel := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChannel)

	// send entries to all followers
	go s.sendToFollowersInParallel(ctx, commitChannel)

	commit := <-commitChannel
	if commit {
		s.lastApplied++
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) sendToFollowersInParallel(ctx context.Context, commitChannel chan bool) {
	fmt.Println("sendToFollowersInParallel")
	targetIdx := s.commitIndex + 1

	response := make(chan bool, len(s.peers)-1)
	for idx := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollowers(idx, ctx, response)
	}

	commitCount := 1
	for {
		// TODO handle crashed nodes
		//s.isCrashedMutex.RLock()
		if s.isCrashed {
			return
		}
		//s.isLeaderMutex.RLock()

		commit := <-response
		if commit {
			commitCount++
		}

		// majority server agrees, commit
		if commitCount > len(s.peers)/2 {
			commitChannel <- true
			s.commitIndex = targetIdx
			break
		}
	}

	for commitCount < len(s.peers) {
		re := <-response
		if re {
			commitCount++
		}
	}

}

func (s *RaftSurfstore) sendToFollowers(serverIdx int, ctx context.Context, response chan bool) {
	fmt.Println("sendToFollowers")

	//s.isCrashedMutex.RLock()
	if s.isCrashed {
		response <- false
		return
	}
	//s.isCrashedMutex.RUnlock()

	addr := s.peers[serverIdx]
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return
	}

	c := NewRaftSurfstoreClient(conn)

	var prevTerm int64
	if s.nextIndex[serverIdx] == 0 {
		prevTerm = -1
	} else {
		prevTerm = s.log[s.nextIndex[serverIdx]-1].Term
	}

	prevLog := s.nextIndex[serverIdx] - 1

	input := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevTerm,
		PrevLogIndex: prevLog,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	out, err := c.AppendEntries(ctx, &input)
	conn.Close()

	if err != nil {
		if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
			return
		}
	}

	if out != nil {
		if out.Success {
			response <- true
			s.nextIndex[serverIdx] = int64(len(s.log))
			s.matchedIndex[serverIdx] = int64(len(s.log) - 1)
		} else if out.Term > s.term {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.term = out.Term
			s.isLeaderMutex.Unlock()
		} else {
			s.nextIndex[serverIdx] = out.MatchedIndex + 1
		}
	}

}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	out := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	// step down
	if input.Term > s.term {
		s.isLeaderMutex.RLock()
		s.isLeader = false
		s.isLeaderMutex.RUnlock()
		s.term = input.Term
	}

	if len(input.Entries) > 0 {

		if int64(len(s.log)) <= input.PrevLogIndex {
			out.MatchedIndex = int64(len(s.log) - 1)
			return out, nil
		}

		//1. Reply false if term < currentTerm (§5.1)
		if input.Term < s.term {
			return out, nil
		}
		//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		//matches prevLogTerm (§5.3)
		if input.PrevLogIndex != -1 {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				for i := input.PrevLogIndex - 1; i >= 0; i-- {
					if s.log[i].Term != s.log[input.PrevLogIndex].Term {
						out.MatchedIndex = i
						break
					}
				}
				return out, nil
			}
		}

		out.Success = true
		s.appendLock.Lock()
		//3. If an existing entry conflicts with a new one (same index but different
		//terms), delete the existing entry and all that follow it (§5.3)
		for idx, entry := range s.log {
			if entry.Term != input.Entries[idx].Term {
				s.log = s.log[:idx]
				break
			}
		}

		//4. Append any new entries not already in the log
		for idx := len(s.log); idx < len(input.Entries); idx++ {
			s.log = append(s.log, input.Entries[idx])
		}
		s.appendLock.Unlock()
		out.MatchedIndex = int64(len(input.Entries) - 1)
	}

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	// TODO only do this if leaderCommit > commitIndex
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	// updating the state machine
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	return out, nil

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("SetLeader")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		fmt.Println("server crashed")
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.RLock()
	s.isLeader = true
	s.isLeaderMutex.RUnlock()

	s.term++

	for id := 0; id < len(s.peers); id++ {
		s.nextIndex[id] = int64(len(s.log))
		s.matchedIndex[id] = -1
	}
	for _, channel := range s.pendingCommits {
		if *channel != nil {
			*channel <- false
		}
	}
	s.pendingCommits = make([]*chan bool, len(s.log))
	return &Success{Flag: true}, nil

}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("heart beat")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	dummy := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	if len(s.log) != 0 {
		dummy.PrevLogTerm = s.log[len(s.log)-1].Term
		dummy.PrevLogIndex = int64(len(s.log) - 1)
	}

	for idx, addr := range s.peers {
		if idx == int(s.id) {
			continue
		}

		nextEntryIdx := s.nextIndex[idx] // next index of cur server
		dummy.PrevLogIndex = nextEntryIdx - 1
		if nextEntryIdx == 0 {
			dummy.PrevLogTerm = -1
		} else {
			dummy.PrevLogTerm = s.log[nextEntryIdx-1].Term
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		defer conn.Close()
		if err != nil {
			return nil, err
		}
		c := NewRaftSurfstoreClient(conn)

		out, err := c.AppendEntries(ctx, &dummy)
		if err != nil {
			continue
		}

		if out != nil {
			if out.Success {
				s.nextIndex[idx] = int64(len(s.log))
				s.matchedIndex[idx] = int64(len(s.log) - 1)
			} else if out.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.term = out.Term
				s.isLeaderMutex.Unlock()
			} else {
				s.nextIndex[idx] = out.MatchedIndex + 1
			}
		}

	}

	temp := s.commitIndex
	for i := s.commitIndex + 1; i < int64(len(s.log)); i++ {
		count := 1
		for idx := range s.peers {
			if int64(idx) != s.id && s.matchedIndex[idx] >= i {
				count++
			}
		}
		if count > len(s.peers)/2 {
			if s.log[i].Term == s.term {
				s.commitIndex = i
			}
		}
	}

	for i := temp + 1; i <= s.commitIndex; i++ {
		if s.pendingCommits[i] == nil {
			break
		}
		*s.pendingCommits[i] <- true
	}

	return &Success{Flag: false}, nil

}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
