package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}
	appendLock := sync.RWMutex{}

	server := RaftSurfstore{
		peers: config.RaftAddrs,
		id:    id,
		ip:    config.RaftAddrs[id],
		//pendingCommits: make([]*chan bool, 0),
		nextIndex:    make([]int64, len(config.RaftAddrs)),
		matchedIndex: make([]int64, len(config.RaftAddrs)),

		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		appendLock:     &appendLock,
		commitIndex:    -1,
		lastApplied:    -1,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {

	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	listener, err := net.Listen("tcp", server.peers[server.id])
	if err != nil {
		return fmt.Errorf("failed to create listener")
	}

	return grpcServer.Serve(listener)
}
