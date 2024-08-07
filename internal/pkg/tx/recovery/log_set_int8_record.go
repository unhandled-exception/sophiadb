package recovery

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type SetInt8LogRecord struct {
	BaseLogRecord

	offset uint32
	value  int8
	block  types.Block
}

func NewSetInt8LogRecord(txnum types.TRX, block types.Block, offset uint32, value int8) SetInt8LogRecord {
	return SetInt8LogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    SetInt8Op,
			txnum: txnum,
		},
		offset: offset,
		value:  value,
		block:  block,
	}
}

func NewSetInt8LogRecordFromBytes(rawRecord []byte) (SetInt8LogRecord, error) {
	r := SetInt8LogRecord{}

	if err := r.unmarshalBytes(rawRecord); err != nil {
		return r, err
	}

	return r, nil
}

func (lr SetInt8LogRecord) Undo(tx trxInt) error {
	if err := tx.Pin(lr.block); err != nil {
		return err
	}

	if err := tx.SetInt8(lr.block, lr.offset, lr.value, false); err != nil {
		return err
	}

	tx.Unpin(lr.block)

	return nil
}

func (lr SetInt8LogRecord) String() string {
	return fmt.Sprintf(
		`<SET_INT8, %d, block: %s, offset: %d, value: %d>`,
		lr.TXNum(),
		lr.block.String(),
		lr.offset,
		lr.value,
	)
}

func (lr SetInt8LogRecord) MarshalBytes() []byte {
	blockFilename := lr.block.Filename

	oppos := uint32(0)
	txpos := oppos + int32Size
	fpos := txpos + int32Size
	bpos := fpos + int32Size + uint32(len(blockFilename))
	ofpos := bpos + int32Size
	vpos := ofpos + int32Size
	recLen := vpos + int8Size

	p := types.NewPage(recLen)

	p.SetUint32(oppos, lr.op)
	p.SetInt32(txpos, int32(lr.txnum))
	p.SetString(fpos, blockFilename)
	p.SetInt32(bpos, int32(lr.block.Number))
	p.SetUint32(ofpos, lr.offset)
	p.SetInt8(vpos, lr.value)

	return p.Content()
}

func (lr *SetInt8LogRecord) unmarshalBytes(rawRecord []byte) error {
	p := types.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)
	lr.txnum = types.TRX(p.GetInt32(int32Size))

	fpos := uint32(2 * int32Size) //nolint:mnd
	blockFilename := p.GetString(fpos)

	bpos := fpos + uint32(int32Size+len(blockFilename))
	blockNum := types.BlockID(p.GetInt32(bpos))

	lr.block = types.Block{Filename: blockFilename, Number: blockNum}

	ofpos := bpos + int32Size
	lr.offset = p.GetUint32(ofpos)

	vpos := ofpos + int32Size
	lr.value = p.GetInt8(vpos)

	return nil
}
