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
	resp := kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, _ := reader.GetCF(req.Cf, req.Key)
	if val == nil {
		resp.NotFound = true
	} else {
		resp.NotFound = false
	}
	resp.Value = val
	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := kvrpcpb.RawPutResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iterator := reader.IterCF(req.Cf)
	iterator.Seek(req.StartKey)
	for count := uint32(0); count < req.Limit && iterator.Valid(); count++ {
		item := iterator.Item()
		kvPair := kvrpcpb.KvPair{}
		kvPair.Key = item.Key()
		kvPair.Value, err = item.Value()
		if err != nil {
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvPair)
		iterator.Next()
	}
	return &resp, nil
}
