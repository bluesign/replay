package replay

import (
	"context"
	"fmt"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/fvm/environment"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/access"
)

type Blocks struct {
	client access.AccessAPIClient
}

func NewBlocks(client access.AccessAPIClient) *Blocks {
	return &Blocks{
		client: client,
	}
}

func (b *Blocks) ByHeightFrom(height uint64, header *flow.Header) (*flow.Header, error) {

	if height == header.Height {
		return header, nil
	}
	if height > header.Height {
		return nil, fmt.Errorf("height %d is more than header height %d", height, header.Height)
	}

	blockResponse, err := b.client.GetBlockByHeight(context.Background(), &access.GetBlockByHeightRequest{
		Height:            height,
		FullBlockResponse: true,
	})
	if err != nil {
		panic(err)
	}

	block, err := convert.MessageToBlock(blockResponse.GetBlock())

	return block.Header, nil
}

var _ environment.Blocks = &Blocks{}
