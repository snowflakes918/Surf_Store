package surfstore

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// ClientSync Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// init Variables
	baseDirFiles, localIndex, remoteIndex, err := initVariables(client)
	if err != nil {
		log.Println(err)
	}

	fmt.Println("hello 1")

	// Sync local index file and baseDirFiles in base directory
	syncBaseLocalDB(client, baseDirFiles, localIndex)
	fmt.Println("hello 2")

	// check for the updates in localIndex and update server
	syncServerWithLocal(client, localIndex, remoteIndex, err)
	fmt.Println("hello 3")

	//check for the updates in Server (other people change) and update local index
	syncLocalWithServer(client, remoteIndex, localIndex)
	fmt.Println("hello 4")

	// rewrite the local index.db
	if err := WriteMetaFile(localIndex, client.BaseDir); err != nil {
		log.Fatal("Error write back to index.db")
	}

	fmt.Println("hello 5 ")

}

func initVariables(client RPCClient) ([]fs.FileInfo, map[string]*FileMetaData, map[string]*FileMetaData, error) {
	// create index.db if not exit
	filePath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		indexDB, e := os.Create(filePath)
		if e != nil {
			log.Fatal("Error creating index.db: ", e)
		}
		indexDB.Close()
	}

	// base dir files
	baseDirFiles, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal("Error reading the base directory: ", err)
	}

	// extract index.db data
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal("Error loading index.db: ", err)
	}

	// get block store address
	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		fmt.Println("Could not get blockStoreAddr: ", err)
	}

	// download server fileInfoMap
	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		fmt.Println("Error getting fileInfoMap from server: ", err)
	}

	return baseDirFiles, localIndex, remoteIndex, nil
}

func syncBaseLocalDB(client RPCClient, files []fs.FileInfo, localIndex map[string]*FileMetaData) error {
	hashMap := make(map[string][]string) // store blockHashList of files in base dir
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}

		// check for empty file
		if file.Size() == 0 { // file is empty
			hashMap[file.Name()] = []string{"-1"}
		} else { // file is not empty
			// how many blocks are needed to store the file
			numOfBlocks := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

			// read the file content
			readingFile, err := os.Open(client.BaseDir + "/" + file.Name())
			if err != nil {
				return err
			}

			// generate hash for this block
			for i := 0; i < numOfBlocks; i++ {
				buffer := make([]byte, client.BlockSize)
				len, err := readingFile.Read(buffer)
				if err != nil {
					return err
				}
				buffer = buffer[:len]
				hash := GetBlockHashString(buffer)
				hashMap[file.Name()] = append(hashMap[file.Name()], hash)
			}

			readingFile.Close()
		}

		// check if the file exits in index.db
		if indexFileMeta, ok := localIndex[file.Name()]; ok {
			// file exits
			if !sameContent(hashMap[file.Name()], indexFileMeta.BlockHashList) {
				// the file has been altered
				localIndex[file.Name()].BlockHashList = hashMap[file.Name()]
				localIndex[file.Name()].Version++
			}
		} else {
			// file does not exit
			fileMeta := FileMetaData{
				Filename:      file.Name(),
				Version:       1,
				BlockHashList: hashMap[file.Name()],
			}

			localIndex[file.Name()] = &fileMeta
		}
	}

	// check for deleted baseDirFiles
	for name, fileMeta := range localIndex {
		if _, ok := hashMap[name]; !ok {
			// file has been deleted, check if it was deleted before
			if !(len(fileMeta.BlockHashList) == 1 && fileMeta.BlockHashList[0] == "0") {
				// file just deleted
				fileMeta.Version++
				fileMeta.BlockHashList = []string{"0"}
			}
		}
	}

	return nil
}

func syncLocalWithServer(client RPCClient, remoteIndex map[string]*FileMetaData, localIndex map[string]*FileMetaData) {
	for name, serverMetaData := range remoteIndex {
		if localMetaData, ok := localIndex[name]; ok {
			// file exits on local
			if localMetaData.Version < serverMetaData.Version {
				// the file has changed by other people
				updateLocal(client, localMetaData, serverMetaData)
			} else if localMetaData.Version == serverMetaData.Version &&
				!sameContent(localMetaData.BlockHashList, serverMetaData.BlockHashList) {
				// the file has changed by other people and locally
				updateLocal(client, localMetaData, serverMetaData)
			}
		} else {
			// file not exits in local
			localIndex[name] = &FileMetaData{}
			updateLocal(client, localIndex[name], serverMetaData)
		}
	}
}

func syncServerWithLocal(client RPCClient, localIndex map[string]*FileMetaData, remoteIndex map[string]*FileMetaData, err error) {
	for name, localFileMeta := range localIndex {
		if serverFileMeta, ok := remoteIndex[name]; ok {
			// file exits on server
			if localFileMeta.Version > serverFileMeta.Version {
				// this file has changed in local
				version := updateServer(client, localFileMeta)
				// handling conflict
				if version == -1 {
					log.Println("update a file error:", err)
					// download the new FileInfoMap from server
					err = client.GetFileInfoMap(&remoteIndex)
					if err != nil {
						log.Println("download new fileInfoMap from server error:", err)
					}
					newFileInfo := remoteIndex[name]
					localIndex[name] = &FileMetaData{Filename: newFileInfo.Filename, Version: newFileInfo.Version, BlockHashList: newFileInfo.BlockHashList}

					// download file from the server
					updateLocal(client, localFileMeta, newFileInfo)
				}
			}
		} else {
			// file not exits on server
			version := updateServer(client, localFileMeta)
			// handling conflict
			if version == -1 {
				log.Println("update a file error:", err)
				// download the new FileInfoMap from server
				err = client.GetFileInfoMap(&remoteIndex)
				if err != nil {
					log.Println("download new fileInfoMap from server error:", err)
				}
				newFileInfo := remoteIndex[name]
				localIndex[name] = &FileMetaData{Filename: newFileInfo.Filename, Version: newFileInfo.Version, BlockHashList: newFileInfo.BlockHashList}

				// download file from the server
				updateLocal(client, localFileMeta, newFileInfo)
			}
		}
	}
}

func updateServer(client RPCClient, data *FileMetaData) int32 {
	var latestVersion int32
	filePath := ConcatPath(client.BaseDir, data.Filename)
	// handling delete conflict
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(data, &latestVersion)
		if err != nil {
			log.Fatal("Could not upload file: ", err)
		}
		data.Version = latestVersion
		return latestVersion
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error occur: ", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatal("Error getting stat of file: ", err)
	}

	// store file into blocks
	n := int(math.Ceil(float64(stat.Size()) / float64(client.BlockSize)))
	blockStoreMap := make(map[string][]string)
	if err := client.GetBlockStoreMap(data.BlockHashList, &blockStoreMap); err != nil {
		fmt.Println("238 err here:", err)
	}

	for i := 0; i < n; i++ {
		buffer := make([]byte, client.BlockSize)
		len, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			log.Println("Error reading bytes from file: ", err)
		}

		block := Block{BlockData: buffer[:len], BlockSize: int32(len)}

		var success bool
		for server, hashList := range blockStoreMap {
			if contains(hashList, data.BlockHashList[i]) {
				// the block belongs to this server
				if err := client.PutBlock(&block, server, &success); err != nil {
					log.Fatal("Error putting block: ", err)
				}

				break
			}
		}

	}

	// update the file
	if err := client.UpdateFile(data, &latestVersion); err != nil {
		// someone already changed file
		fmt.Println("Error updating the file on the server: ", err)
	}

	return latestVersion

}

func updateLocal(client RPCClient, localMeta *FileMetaData, serverMeta *FileMetaData) error {
	filePath := ConcatPath(client.BaseDir, serverMeta.Filename)
	// create file in base dir
	file, err := os.Create(filePath)
	if err != nil {
		log.Println("Error creating file: ", err)
	}
	defer file.Close()

	*localMeta = *serverMeta

	//File deleted in server
	if len(serverMeta.BlockHashList) == 1 && serverMeta.BlockHashList[0] == "0" {
		if err := os.Remove(filePath); err != nil {
			log.Fatal("Could not remove local file: ", err)
			return err
		}
		return nil
	}

	// get the block data and write to local
	blockStoreMap := make(map[string][]string)
	if err := client.GetBlockStoreMap(serverMeta.BlockHashList, &blockStoreMap); err != nil {
		panic(err)
	}

	for _, hash := range serverMeta.BlockHashList {
		var block Block

		for server, hashList := range blockStoreMap {
			// check if the hash is in hashList
			if contains(hashList, hash) {
				if err := client.GetBlock(hash, server, &block); err != nil {
					fmt.Println("Error getting block: ", err)
				}

				break
			}
		}

		_, err := file.Write(block.BlockData)
		if err != nil {
			fmt.Println("Error writing data to base dir:", err)
		}
	}

	return nil
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func sameContent(hash1 []string, hash2 []string) bool {
	return reflect.DeepEqual(hash1, hash2)
}
