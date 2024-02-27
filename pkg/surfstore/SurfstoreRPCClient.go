package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn) // protoc gen

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// stores the block
	out, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = out.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn) // protoc gen

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	out, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = out.Hashes
	return conn.Close()

}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		out, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				conn.Close()
				continue
			}
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
				fmt.Println("DeadlineExceeded 2")
				os.Exit(1)
			}
			conn.Close()
			return err
		}

		*serverFileInfoMap = out.FileInfoMap
		conn.Close()
		break
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		out, err := c.UpdateFile(ctx, fileMetaData)
		fmt.Println("out:  ", out)
		fmt.Println("err:  ", err)
		if err != nil {
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				conn.Close()
				continue
			}
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
				fmt.Println("DeadlineExceeded 1")
				os.Exit(1)
			}
			conn.Close()
			return err
		}

		*latestVersion = out.Version
		conn.Close()
		break
	}

	return nil

}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	out, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	*blockHashes = out.Hashes
	return conn.Close()

}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		out, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})

		if err != nil {
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				conn.Close()
				continue
			}
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
				fmt.Println("DeadlineExceeded")
				os.Exit(1)
			}
			conn.Close()
			return err
		}

		bsMap := out.BlockStoreMap
		temp := make(map[string][]string)

		for block, blockHash := range bsMap {
			temp[block] = blockHash.Hashes
		}

		*blockStoreMap = temp

		conn.Close()
		break
	}

	return nil

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		out, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				conn.Close()
				continue
			}
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
				fmt.Println("DeadlineExceeded")
				os.Exit(1)
			}
			conn.Close()
			return err
		}

		*blockStoreAddrs = out.BlockStoreAddrs
		conn.Close()
		break
	}

	return nil

}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
