package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	mutex sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	var infoMap FileInfoMap
	infoMap.FileInfoMap = m.FileMetaMap
	return &infoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, ok := m.FileMetaMap[filename]; ok {
		// handling edit conflict
		if version == m.FileMetaMap[filename].Version+1 {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
			return &Version{Version: version}, errors.New("Version is too old")
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
	}

	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := BlockStoreMap{BlockStoreMap: map[string]*BlockHashes{}}
	responsibleServer := ""

	for _, h := range blockHashesIn.Hashes {
		responsibleServer = m.ConsistentHashRing.GetResponsibleServer(h)
		if blockStoreMap.BlockStoreMap[responsibleServer] == nil {
			blockStoreMap.BlockStoreMap[responsibleServer] = &BlockHashes{Hashes: make([]string, 0)}
		}
		blockStoreMap.BlockStoreMap[responsibleServer].Hashes = append(blockStoreMap.BlockStoreMap[responsibleServer].Hashes, h)
	}

	return &blockStoreMap, nil

}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
