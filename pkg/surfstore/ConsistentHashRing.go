package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	// ServerMap[hash] = serverName
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	consistentHashRing := c.ServerMap
	serverHashes := []string{}

	for h := range consistentHashRing {
		serverHashes = append(serverHashes, h)
	}

	sort.Strings(serverHashes)
	responsibleServer := ""
	for _, h := range serverHashes {
		if h > blockId {
			responsibleServer = consistentHashRing[h]
			break
		}
	}

	if responsibleServer == "" {
		responsibleServer = consistentHashRing[serverHashes[0]]
	}

	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentHashRing := ConsistentHashRing{}

	serverMap := make(map[string]string)
	for _, serverAddr := range serverAddrs {
		serverHash := consistentHashRing.Hash("blockstore" + serverAddr)
		serverMap[serverHash] = serverAddr
	}

	consistentHashRing.ServerMap = serverMap

	return &consistentHashRing
}
