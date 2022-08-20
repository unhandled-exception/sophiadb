package transaction_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

type TRXGeneratorTestSute struct {
	suite.Suite
}

func TestTRXGeneratorTestSute(t *testing.T) {
	suite.Run(t, new(TRXGeneratorTestSute))
}

func (ts *TRXGeneratorTestSute) TestSetLastTRX() {
	t := ts.T()
	newLastTRXValue := types.TRX(588345345)

	sut := transaction.NewTRXGenerator()
	sut.SetLastTRX(newLastTRXValue)

	assert.EqualValues(t, newLastTRXValue+1, sut.NextTRX())
}

func (ts *TRXGeneratorTestSute) TestGenerateSequence() {
	t := ts.T()
	sut := transaction.NewTRXGenerator()

	workers := 20
	iterations := 100

	m := sync.Mutex{}
	res := make([]types.TRX, 0, workers*iterations)

	wg := sync.WaitGroup{}
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < iterations; i++ {
				trxID := sut.NextTRX()

				m.Lock()
				res = append(res, trxID)
				m.Unlock()
			}

			wg.Done()
		}()
	}

	wg.Wait()

	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})

	require.Len(t, res, workers*iterations)
	assert.EqualValues(t, res[0], 1)
	assert.EqualValues(t, workers*iterations, res[len(res)-1])
}
