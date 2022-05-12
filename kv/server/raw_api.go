package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, _ := reader.GetCF(req.Cf, req.Key)
	exist := false
	if value == nil {
		exist = true
	}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: exist}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}}
	modifys := []storage.Modify{modify}
	err := server.storage.Write(nil, modifys)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{Data: storage.Delete{Key: req.Key, Cf: req.Cf}}
	modifys := []storage.Modify{modify}
	err := server.storage.Write(nil, modifys)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iterator := reader.IterCF(req.Cf)
	iterator.Seek(req.StartKey)
	var kvPairs []*kvrpcpb.KvPair
	for i := 0; uint32(i) < req.Limit; i++ {
		if iterator.Valid() == false {
			break
		}
		item := iterator.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvPair := &kvrpcpb.KvPair{Key: key, Value: value}
		kvPairs = append(kvPairs, kvPair)
		iterator.Next()
	}
	iterator.Close()
	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
