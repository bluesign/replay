package replay

import (
	"encoding/hex"
	"fmt"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/fvm/environment"
	flowgo "github.com/onflow/flow-go/model/flow"
)

type EntropyProvider struct {
	seed  []byte
	error error
}

func (e EntropyProvider) RandomSource() ([]byte, error) {
	return e.seed, e.error
}

func NewEntropyProvider(next *flowgo.Header) environment.EntropyProvider {
	packer := model.SigDataPacker{}
	sigData, err := packer.Decode(next.ParentVoterSigData)
	if err != nil {
		fmt.Println("error getting entropy seed")
		return nil
	}
	fmt.Println("beacon:", hex.EncodeToString(sigData.ReconstructedRandomBeaconSig))
	return EntropyProvider{seed: sigData.ReconstructedRandomBeaconSig, error: err}
}
