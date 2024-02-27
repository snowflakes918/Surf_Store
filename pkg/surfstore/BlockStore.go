package surfstore

import (
	context "context"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mutex sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	block := bs.BlockMap[blockHash.Hash]
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	bs.BlockMap[hash] = block
	var success Success
	success.Flag = true
	return &success, nil
}

// GetBlockHashes Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := BlockHashes{}
	for hash := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, hash)
	}

	return &blockHashes, nil
}

// HasBlocks Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var blockHashesOut BlockHashes

	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hash)
		}
	}

	return &blockHashesOut, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
