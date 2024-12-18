package client

import (
	"context"
	"fmt"
	"github.com/onflow/flow-go/fvm/storage/snapshot"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"

	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"github.com/onflow/flow/protobuf/go/flow/access"
	executiondata "github.com/onflow/flow/protobuf/go/flow/executiondata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BlockFollower struct {
	client access.AccessAPIClient
	chain  flow.Chain
}

type BlockDataResponse struct {
	Header *flow.Header
}

func NewBlockFollower(address string, chain flow.Chain, opts ...grpc.DialOption) (*BlockFollower, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &BlockFollower{
		client: access.NewAccessAPIClient(conn),
		chain:  chain,
	}, nil
}

func (c *BlockFollower) SubscribeBlockData(ctx context.Context, startHeight uint64, opts ...grpc.CallOption) (*Subscription[BlockDataResponse], error) {

	req := access.SubscribeBlockHeadersFromStartHeightRequest{
		StartBlockHeight: startHeight,
		BlockStatus:      entities.BlockStatus_BLOCK_SEALED,
	}

	stream, err := c.client.SubscribeBlockHeadersFromStartHeight(ctx, &req, opts...)
	if err != nil {
		return nil, err
	}

	sub := NewSubscription[BlockDataResponse]()
	go func() {
		defer close(sub.ch)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				sub.err = fmt.Errorf("error receiving execution data: %w", err)
				return
			}
			//fmt.Println(resp.Block)
			header, err := convert.MessageToBlockHeader(resp.GetHeader())
			if err != nil {
				log.Printf("error converting block data:\n%v", header)
				sub.err = fmt.Errorf("error converting block data: %w", err)
				return
			}
			if header.Height%1000 == 0 {
				log.Printf("received block data for block %d", header.Height)
			}
			sub.ch <- BlockDataResponse{
				Header: header,
			}
		}
	}()

	return sub, nil
}

type ExecutionDataClient struct {
	client executiondata.ExecutionDataAPIClient
	chain  flow.Chain
	cache  map[flow.RegisterID]flow.RegisterValue
}

func NewExecutionDataClient(address string, chain flow.Chain, opts ...grpc.DialOption) (*ExecutionDataClient, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &ExecutionDataClient{
		client: executiondata.NewExecutionDataAPIClient(conn),
		chain:  chain,
		cache:  make(map[flow.RegisterID]flow.RegisterValue),
	}, nil
}

// GetExecutionDataForBlockID returns the BlockExecutionData for the given block ID.
func (c *ExecutionDataClient) GetExecutionDataForBlockID(
	ctx context.Context,
	blockID flow.Identifier,
	opts ...grpc.CallOption,
) (*execution_data.BlockExecutionData, error) {
	req := &executiondata.GetExecutionDataByBlockIDRequest{
		BlockId: blockID[:],
	}
	resp, err := c.client.GetExecutionDataByBlockID(ctx, req, opts...)
	if err != nil {
		return nil, err
	}

	execData, err := convert.MessageToBlockExecutionData(resp.GetBlockExecutionData(), c.chain)
	if err != nil {
		return nil, err
	}

	return execData, nil
}

type ExecutionDataResponse struct {
	BlockID       flow.Identifier
	Height        uint64
	ExecutionData *execution_data.BlockExecutionData
}

// SubscribeExecutionData subscribes to execution data updates starting at the given block ID or height.
func (c *ExecutionDataClient) SubscribeExecutionData(
	ctx context.Context,
	startBlockID flow.Identifier,
	startHeight uint64,
	opts ...grpc.CallOption,
) (*Subscription[ExecutionDataResponse], error) {
	if startBlockID != flow.ZeroID && startHeight > 0 {
		return nil, fmt.Errorf("cannot specify both start block ID and start height")
	}

	req := executiondata.SubscribeExecutionDataRequest{
		EventEncodingVersion: entities.EventEncodingVersion_CCF_V0,
	}
	if startBlockID != flow.ZeroID {
		req.StartBlockId = startBlockID[:]
	}
	if startHeight > 0 {
		req.StartBlockHeight = startHeight
	}

	stream, err := c.client.SubscribeExecutionData(ctx, &req, opts...)
	if err != nil {
		return nil, err
	}

	sub := NewSubscription[ExecutionDataResponse]()
	go func() {
		defer close(sub.ch)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				sub.err = fmt.Errorf("error receiving execution data: %w [%d]", err, startHeight)
				return
			}

			execData, err := convert.MessageToBlockExecutionData(resp.GetBlockExecutionData(), c.chain)
			if err != nil {
				log.Printf("error converting execution data:\n%v", resp.GetBlockExecutionData())
				sub.err = fmt.Errorf("error converting execution data: %w", err)
				return
			}

			if resp.BlockHeight%1000 == 0 {
				log.Printf("received execution data for block %d %x with %d chunks", resp.BlockHeight, execData.BlockID, len(execData.ChunkExecutionDatas))
			}

			sub.ch <- ExecutionDataResponse{
				Height:        resp.BlockHeight,
				ExecutionData: execData,
			}
		}
	}()

	return sub, nil
}

func (c *ExecutionDataClient) Set(
	key flow.RegisterID,
	value flow.RegisterValue,
) {
	c.cache[key] = value
}

func (c *ExecutionDataClient) LedgerByHeight(
	ctx context.Context,
	blockHeight uint64,
) (snapshot.StorageSnapshot, error) {
	return snapshot.NewReadFuncStorageSnapshot(func(id flow.RegisterID) (flow.RegisterValue, error) {
		// create a copy so updating it doesn't affect future calls
		lookupHeight := blockHeight

		// first try to see if we have local stored ledger
		v, ok := c.cache[id]
		if ok {
			return v, nil
		}

		// FVM expects an empty byte array if the value is not found
		value := []byte{}

		registerID := convert.RegisterIDToMessage(flow.RegisterID{Key: id.Key, Owner: id.Owner})
		response, err := c.client.GetRegisterValues(ctx, &executiondata.GetRegisterValuesRequest{
			BlockHeight: lookupHeight,
			RegisterIds: []*entities.RegisterID{registerID},
		})
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return nil, err
			}

		}

		if response != nil && len(response.Values) > 0 {
			value = response.Values[0]
		} else {
			fmt.Println("Register not found", id.String())
		}

		c.cache[id] = value

		return value, nil
	}), nil
}

type EventFilter struct {
	EventTypes []string
	Addresses  []string
	Contracts  []string
}

type EventsResponse struct {
	Height  uint64
	BlockID flow.Identifier
	Events  []flow.Event
}

func (c *ExecutionDataClient) SubscribeEvents(
	ctx context.Context,
	startBlockID flow.Identifier,
	startHeight uint64,
	filter EventFilter,
	opts ...grpc.CallOption,
) (*Subscription[EventsResponse], error) {
	if startBlockID != flow.ZeroID && startHeight > 0 {
		return nil, fmt.Errorf("cannot specify both start block ID and start height")
	}

	req := executiondata.SubscribeEventsRequest{
		Filter: &executiondata.EventFilter{
			EventType: filter.EventTypes,
			Address:   filter.Addresses,
			Contract:  filter.Contracts,
		},
	}
	if startBlockID != flow.ZeroID {
		req.StartBlockId = startBlockID[:]
	}
	if startHeight > 0 {
		req.StartBlockHeight = startHeight
	}

	stream, err := c.client.SubscribeEvents(ctx, &req, opts...)
	if err != nil {
		return nil, err
	}

	sub := NewSubscription[EventsResponse]()
	go func() {
		defer close(sub.ch)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				sub.err = fmt.Errorf("error receiving execution data: %w", err)
				return
			}

			sub.ch <- EventsResponse{
				Height:  resp.GetBlockHeight(),
				BlockID: convert.MessageToIdentifier(resp.GetBlockId()),
				Events:  convert.MessagesToEvents(resp.GetEvents()),
			}
		}
	}()

	return sub, nil
}
