package recovery

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type SetStringLogRecord struct {
	BaseLogRecord

	offset uint32
	value  string
	block  types.Block
}

func NewSetStringLogRecord(txnum types.TRX, block types.Block, offset uint32, value string) SetStringLogRecord {
	return SetStringLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    SetStringOp,
			txnum: txnum,
		},
		offset: offset,
		value:  value,
		block:  block,
	}
}

func NewSetStringLogRecordFromBytes(rawRecord []byte) (SetStringLogRecord, error) {
	r := SetStringLogRecord{}

	if err := r.unmarshalBytes(rawRecord); err != nil {
		return r, err
	}

	return r, nil
}

func (lr SetStringLogRecord) Undo(tx trxInt) error {
	if err := tx.Pin(lr.block); err != nil {
		return err
	}

	if err := tx.SetString(lr.block, lr.offset, lr.value, false); err != nil {
		return err
	}

	tx.Unpin(lr.block)

	return nil
}

func (lr SetStringLogRecord) String() string {
	return fmt.Sprintf(
		`<SET_STRING, %d, block: %s, offset: %d, value: "%s">`,
		lr.TXNum(),
		lr.block.String(),
		lr.offset,
		lr.value,
	)
}

func (lr SetStringLogRecord) MarshalBytes() []byte {
	blockFilename := lr.block.Filename

	oppos := uint32(0)
	txpos := oppos + int32Size
	fpos := txpos + int32Size
	bpos := fpos + int32Size + uint32(len(blockFilename))
	ofpos := bpos + int32Size
	vpos := ofpos + int32Size
	recLen := vpos + int32Size + uint32(len(lr.value))

	p := types.NewPage(recLen)

	p.SetUint32(oppos, lr.op)
	p.SetInt32(txpos, int32(lr.txnum))
	p.SetString(fpos, blockFilename)
	p.SetInt32(bpos, int32(lr.block.Number))
	p.SetUint32(ofpos, lr.offset)
	p.SetString(vpos, lr.value)

	return p.Content()
}

func (lr *SetStringLogRecord) unmarshalBytes(rawRecord []byte) error {
	p := types.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)
	lr.txnum = types.TRX(p.GetInt32(int32Size))

	fpos := uint32(2 * int32Size) //nolint:gomnd
	blockFilename := p.GetString(fpos)

	bpos := fpos + uint32(int32Size+len(blockFilename))
	blockNum := types.BlockID(p.GetUint32(bpos))

	lr.block = types.Block{Filename: blockFilename, Number: blockNum}

	ofpos := bpos + int32Size
	lr.offset = p.GetUint32(ofpos)

	vpos := ofpos + int32Size
	lr.value = p.GetString(vpos)

	return nil
}
