package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/bluesign/replay/client"
	"github.com/bluesign/replay/replay"
	"github.com/onflow/cadence/encoding/ccf"
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

func main() {
	ctx := context.Background()
	accessUrl := "access-007.mainnet26.nodes.onflow.org:9000"
	chain := flow.Mainnet.Chain()

	conn, err := grpc.Dial(accessUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	api := access.NewAccessAPIClient(conn)

	resp, err := api.GetLatestBlockHeader(ctx, &access.GetLatestBlockHeaderRequest{})
	if err != nil {
		panic(err)
	}
	height := resp.GetBlock().Height - 1000

	execFollower, err := client.NewExecutionDataClient(
		accessUrl,
		chain,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1024*1024*100),
			grpc.UseCompressor(gzip.Name)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		panic(err)
	}
	execSubscription, err := execFollower.SubscribeExecutionData(ctx, flow.ZeroID, uint64(height))

	if err != nil {
		panic(err)
	}

	for {
		select {
		case executionData, ok := <-execSubscription.Channel():
			if execSubscription.Err() != nil || !ok {
				panic(execSubscription.Err())
			}
			fmt.Println("Received execution:", executionData.Height)

			current, err := api.GetBlockByHeight(context.Background(), &access.GetBlockByHeightRequest{
				Height:            executionData.Height,
				FullBlockResponse: true,
			})
			if err != nil {
				panic(err)
			}

			currentBlock, err := convert.MessageToBlock(current.GetBlock())
			if err != nil {
				panic(err)
			}

			next, err := api.GetBlockByHeight(context.Background(), &access.GetBlockByHeightRequest{
				Height:            executionData.Height + 1,
				FullBlockResponse: true,
			})
			if err != nil {
				panic(err)
			}

			nextBlock, err := convert.MessageToBlock(next.GetBlock())
			if err != nil {
				panic(err)
			}

			var entropyProvider = replay.NewEntropyProvider(nextBlock.Header)

			blocks := replay.NewBlocks(api)

			snap, err := execFollower.LedgerByHeight(ctx, executionData.Height-1)
			if err != nil {
				panic(err)
			}

			blockDatabase := fvmStorage.NewBlockDatabase(snap, 0, nil)
			index := 0
			fmt.Println("Processing Block", currentBlock.Header.Height, currentBlock.ID())
			for ci, chunk := range executionData.ExecutionData.ChunkExecutionDatas {

				fvmContext := fvm.NewContext(
					fvm.WithChain(chain),
					fvm.WithBlockHeader(currentBlock.Header),
					fvm.WithBlocks(blocks),
					fvm.WithTransactionFeesEnabled(true),
					fvm.WithEntropyProvider(entropyProvider),
					fvm.WithCadenceLogging(true),
					fvm.WithAccountStorageLimit(true),
					fvm.WithAuthorizationChecksEnabled(true),
					fvm.WithSequenceNumberCheckAndIncrementEnabled(true),
					fvm.WithEVMEnabled(true),
					fvm.WithMemoryLimit(4*1024*1024*1024), //4GB
					fvm.WithComputationLimit(10_000),      //10k
				)

				var writes map[flow.RegisterID]flow.RegisterValue = make(map[flow.RegisterID]flow.RegisterValue)

				for i, transaction := range chunk.Collection.Transactions {
					txResult := chunk.TransactionResults[i]

					fmt.Println("transaction", index, transaction.ID(), txResult.TransactionID)

					if ci == len(executionData.ExecutionData.ChunkExecutionDatas)-1 {
						//system transaction
						fvmContext = fvm.NewContextFromParent(
							fvmContext,
							fvm.WithContractDeploymentRestricted(false),
							fvm.WithContractRemovalRestricted(false),
							fvm.WithAuthorizationChecksEnabled(false),
							fvm.WithSequenceNumberCheckAndIncrementEnabled(false),
							fvm.WithTransactionFeesEnabled(false),
							fvm.WithEventCollectionSizeLimit(computer.SystemChunkEventCollectionMaxSize),
							fvm.WithMemoryAndInteractionLimitsDisabled(),
							// only the system transaction is allowed to call the block entropy provider
							fvm.WithRandomSourceHistoryCallAllowed(true),
						)
					}
					proc := fvm.Transaction(transaction, uint32(index))

					txnState, err := blockDatabase.NewTransaction(proc.ExecutionTime(), fvmState.DefaultParameters())
					if err != nil {
						panic(err)
					}
					index++

					executor := proc.NewExecutor(fvmContext, txnState)
					err = fvm.Run(executor)

					if err != nil {
						fmt.Println(err)
					}

					// check computation used
					if txResult.ComputationUsed != executor.Output().ComputationUsed {
						panic("Computation used does not match")
					}

					// check error
					if executor.Output().Err != nil && !txResult.Failed {
						panic("error mismatch")
					}

					// check events
					for _, event := range executor.Output().Events {
						found := false
						for _, chunkEvent := range chunk.Events {
							if event.TransactionID == chunkEvent.TransactionID && event.EventIndex == chunkEvent.EventIndex {
								found = true

								if event.Type != chunkEvent.Type {
									fmt.Println("mismatched event type", event.Type, chunkEvent.Type)

									panic("mismatched event type")
								}
								if !bytes.Equal(event.Payload, chunkEvent.Payload) {
									fmt.Println(hex.EncodeToString(event.Payload), hex.EncodeToString(chunkEvent.Payload))
									v, _ := ccf.NewDecoder(nil, event.Payload).Decode()
									c, _ := ccf.NewDecoder(nil, chunkEvent.Payload).Decode()
									fmt.Println(v)
									fmt.Println(c)

									panic("mismatched event payload")
								}
							}
						}
						if !found {
							fmt.Println("Event", event)
							panic("missing event")
						}
					}

					if err != nil {
						panic(err)
					}

					txnState.Finalize()
					resultSnapshot, err := txnState.Commit()

					for regID, regValue := range resultSnapshot.WriteSet {
						writes[regID] = regValue
						execFollower.Set(regID, regValue)
					}

					if err != nil {
						panic(err)
					}
				}

				for _, update := range chunk.TrieUpdate.Payloads {
					if update == nil {
						continue
					}
					key, err := update.Key()
					if err != nil {
						panic(err)
					}

					rid, err := convert2.LedgerKeyToRegisterID(key)
					if err != nil {
						panic(err)
					}

					written, ok := writes[rid]
					if !ok {
						fmt.Println("rid:", rid)
						v, _ := snap.Get(rid)
						fmt.Println(hex.EncodeToString(v), hex.EncodeToString(update.Value()))
						panic("missing write")
					}

					if !bytes.Equal(written, update.Value()) {
						fmt.Println("rid:", rid)
						fmt.Println(hex.EncodeToString(written), hex.EncodeToString(update.Value()))
						panic("different write")
					}
					delete(writes, rid)

				}

				if len(writes) > 0 {
					panic("missing writes2")
				}

			}

			if !ok {
				break
			}
		}
	}
}
