package recovery

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type SetInt64LogRecord struct {
	BaseLogRecord

	offset uint32
	value  int64
	block  types.Block
}

func NewSetInt64LogRecord(txnum types.TRX, block types.Block, offset uint32, value int64) *SetInt64LogRecord {
	return &SetInt64LogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    SetInt64Op,
			txnum: txnum,
		},
		offset: offset,
		value:  value,
		block:  block,
	}
}

func NewSetInt64LogRecordFromBytes(rawRecord []byte) (*SetInt64LogRecord, error) {
	r := SetInt64LogRecord{}

	err := r.unmarshalBytes(rawRecord)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (lr *SetInt64LogRecord) Undo(tx trxInt) error {
	if err := tx.Pin(lr.block); err != nil {
		return err
	}

	if err := tx.SetInt64(lr.block, lr.offset, lr.value, false); err != nil {
		return err
	}

	tx.Unpin(lr.block)

	return nil
}

func (lr *SetInt64LogRecord) String() string {
	return fmt.Sprintf(
		`<SET_INT64, %d, block: %s, offset: %d, value: %d>`,
		lr.TXNum(),
		lr.block.String(),
		lr.offset,
		lr.value,
	)
}

func (lr *SetInt64LogRecord) MarshalBytes() []byte {
	blockFilename := lr.block.Filename

	oppos := uint32(0)
	txpos := oppos + int32Size
	fpos := txpos + int32Size
	bpos := fpos + int32Size + uint32(len(blockFilename))
	ofpos := bpos + int32Size
	vpos := ofpos + int32Size
	recLen := vpos + int64Size

	p := types.NewPage(recLen)

	p.SetUint32(oppos, lr.op)
	p.SetInt32(txpos, int32(lr.txnum))
	p.SetString(fpos, blockFilename)
	p.SetInt32(bpos, lr.block.Number)
	p.SetUint32(ofpos, lr.offset)
	p.SetInt64(vpos, lr.value)

	return p.Content()
}

func (lr *SetInt64LogRecord) unmarshalBytes(rawRecord []byte) error {
	p := types.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)
	lr.txnum = types.TRX(p.GetInt32(int32Size))

	fpos := uint32(2 * int32Size) //nolint:gomnd
	blockFilename := p.GetString(fpos)

	bpos := fpos + uint32(int32Size+len(blockFilename))
	blockNum := p.GetInt32(bpos)

	lr.block = types.NewBlock(blockFilename, blockNum)

	ofpos := bpos + int32Size
	lr.offset = p.GetUint32(ofpos)

	vpos := ofpos + int32Size
	lr.value = p.GetInt64(vpos)

	return nil
}
