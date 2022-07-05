package transaction

import (
	"sync/atomic"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type TRXGenerator struct {
	lastTRX *types.TRX
}

func NewTRXGenerator() *TRXGenerator {
	var initVal types.TRX = 0

	return &TRXGenerator{
		lastTRX: &initVal,
	}
}

func (g *TRXGenerator) SetLastTRX(lastTRX types.TRX) {
	atomic.StoreInt32((*int32)(g.lastTRX), int32(lastTRX))
}

func (g *TRXGenerator) NextTRX() types.TRX {
	return types.TRX(atomic.AddInt32((*int32)(g.lastTRX), 1))
}
