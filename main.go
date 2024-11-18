package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/bluesign/replay/client"
	"github.com/bluesign/replay/replay"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/engine/execution/computation/computer"
	"github.com/onflow/flow-go/fvm"
	fvmStorage "github.com/onflow/flow-go/fvm/storage"
	fvmState "github.com/onflow/flow-go/fvm/storage/state"
	convert2 "github.com/onflow/flow-go/ledger/common/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/access"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx := context.Background()
	accessUrl := "access-007.mainnet26.nodes.onflow.org:9000"
	chain := flow.Mainnet.Chain()

	conn, err := grpc.Dial(accessUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	panicOnError(err)
	api := access.NewAccessAPIClient(conn)

	resp, err := api.GetLatestBlockHeader(ctx, &access.GetLatestBlockHeaderRequest{})
	panicOnError(err)
	height := resp.GetBlock().Height - 1000

	execFollower, err := client.NewExecutionDataClient(
		accessUrl,
		chain,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1024*1024*100),
			grpc.UseCompressor(gzip.Name)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	panicOnError(err)

	execSubscription, err := execFollower.SubscribeExecutionData(ctx, flow.ZeroID, uint64(height))
	panicOnError(err)

	for {
		select {
		case executionData, ok := <-execSubscription.Channel():
			if execSubscription.Err() != nil || !ok {
				panic(execSubscription.Err())
			}
			fmt.Println("Received execution for height:", executionData.Height)

			current, err := api.GetBlockByHeight(context.Background(), &access.GetBlockByHeightRequest{
				Height:            executionData.Height,
				FullBlockResponse: true,
			})
			panicOnError(err)

			currentBlock, err := convert.MessageToBlock(current.GetBlock())
			panicOnError(err)

			next, err := api.GetBlockByHeight(context.Background(), &access.GetBlockByHeightRequest{
				Height:            executionData.Height + 1,
				FullBlockResponse: true,
			})
			panicOnError(err)

			nextBlock, err := convert.MessageToBlock(next.GetBlock())
			panicOnError(err)

			fvmContext := fvm.NewContext(
				fvm.WithChain(chain),
				fvm.WithBlockHeader(currentBlock.Header),
				fvm.WithBlocks(replay.NewBlocks(api)),
				fvm.WithTransactionFeesEnabled(true),
				fvm.WithEntropyProvider(replay.NewEntropyProvider(nextBlock.Header)),
				fvm.WithCadenceLogging(false),
				fvm.WithAccountStorageLimit(true),
				fvm.WithAuthorizationChecksEnabled(true),
				fvm.WithSequenceNumberCheckAndIncrementEnabled(true),
				fvm.WithEVMEnabled(true),
				fvm.WithMemoryLimit(4*1024*1024*1024), //4GB
				fvm.WithComputationLimit(10_000),      //10k
			)

			snap, err := execFollower.LedgerByHeight(ctx, executionData.Height-1)
			panicOnError(err)

			blockDatabase := fvmStorage.NewBlockDatabase(snap, 0, nil)
			index := 0
			fmt.Println("Processing Block", currentBlock.Header.Height, currentBlock.ID())

			if len(executionData.ExecutionData.ChunkExecutionDatas) != 1 {
				continue
			}
			for ci, chunk := range executionData.ExecutionData.ChunkExecutionDatas {

				for _, update := range chunk.TrieUpdate.Payloads {
					if update == nil {
						continue
					}

					key, _ := update.Key()
					if hex.EncodeToString(key.KeyParts[0].Value) == "d421a63faae318f9" {
						fmt.Println(hex.EncodeToString(key.KeyParts[0].Value), string(key.KeyParts[1].Value), hex.EncodeToString(update.Value()))
					}

				}

				var writes = make(map[flow.RegisterID]flow.RegisterValue)
				eventIndex := 0

				for i, transaction := range chunk.Collection.Transactions {

					fmt.Println("transaction", index, transaction.ID())
					txResult := chunk.TransactionResults[i]
					if transaction.ID() != txResult.TransactionID {
						panic(fmt.Sprintf("invalid transaction ID: %s vs %s", transaction.ID(), txResult.TransactionID))
					}
					proc := fvm.Transaction(transaction, uint32(index))
					index++
					txnState, err := blockDatabase.NewTransaction(proc.ExecutionTime(), fvmState.DefaultParameters())
					if err != nil {
						panic(err)
					}

					//system transaction
					if ci == len(executionData.ExecutionData.ChunkExecutionDatas)-1 {
						fvmContext = computer.SystemChunkContext(fvmContext, nil)
					}

					//execute transaction
					executor := proc.NewExecutor(fvmContext, txnState)
					panicOnError(fvm.Run(executor))

					// check error
					if executor.Output().Err != nil && !txResult.Failed {
						panic(fmt.Sprintf("error mismatch: %s", executor.Output().Err))
					}

					// check computation used
					if txResult.ComputationUsed != executor.Output().ComputationUsed {
						panic(fmt.Sprintf("Computation used does not match: %d vs %d",
							txResult.ComputationUsed,
							executor.Output().ComputationUsed,
						))
					}

					for _, event := range executor.Output().Events {
						if eventIndex > len(chunk.Events)-1 {
							panic("Extra event emitted")
						}
						chunkEvent := chunk.Events[eventIndex]
						if !bytes.Equal(chunkEvent.Fingerprint(), event.Fingerprint()) {
							panic("Event fingerprint mismatch")
						}
						eventIndex++
					}

					panicOnError(txnState.Finalize())
					resultSnapshot, err := txnState.Commit()

					panicOnError(err)

					for regID, regValue := range resultSnapshot.WriteSet {
						writes[regID] = regValue
						execFollower.Set(regID, regValue)
					}

				}

				for _, update := range chunk.TrieUpdate.Payloads {
					if update == nil {
						continue
					}

					key, err := update.Key()

					fmt.Println(string(key.KeyParts[1].Value), hex.EncodeToString(update.Value()))

					panicOnError(err)

					rid, err := convert2.LedgerKeyToRegisterID(key)
					panicOnError(err)

					written, ok := writes[rid]
					if !ok {
						panic(fmt.Sprintf("missing write: %s", rid))
					}
					if !bytes.Equal(written, update.Value()) {
						panic(fmt.Sprintf("different write: %s %s <-> %s ", rid, hex.EncodeToString(update.Value()), hex.EncodeToString(written)))
					}
					delete(writes, rid)
				}

				if len(writes) > 0 {
					panic("missing writes")
				}

				panic("should have paniced")

				if len(chunk.Events) != eventIndex {
					panic("Event count mismatch")
				}
			}
		}
	}
}
