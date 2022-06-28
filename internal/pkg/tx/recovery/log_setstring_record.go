package recovery

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

type SetStringLogRecord struct {
	BaseLogRecord

	offset uint32
	value  string
	block  *storage.BlockID
}

func NewSetStringLogRecord(txnum int32, block *storage.BlockID, offset uint32, value string) *SetStringLogRecord {
	return &SetStringLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    SetStringOp,
			txnum: txnum,
		},
		offset: offset,
		value:  value,
		block:  block,
	}
}

func NewSetStringLogRecordFromBytes(rawRecord []byte) (*SetStringLogRecord, error) {
	r := SetStringLogRecord{}

	err := r.unmarshalBytes(rawRecord)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (lr *SetStringLogRecord) Undo(tx trxInt) error {
	if err := tx.Pin(lr.block); err != nil {
		return err
	}

	if err := tx.SetString(lr.block, lr.offset, lr.value, false); err != nil {
		return err
	}

	if err := tx.Unpin(lr.block); err != nil {
		return err
	}

	return nil
}

func (lr *SetStringLogRecord) String() string {
	return fmt.Sprintf(
		`<SET_STRING, %d, block: %s, offset: %d, value: "%s">`,
		lr.TXNum(),
		lr.block.String(),
		lr.offset,
		lr.value,
	)
}

func (lr *SetStringLogRecord) MarshalBytes() []byte {
	blockFilename := lr.block.Filename()

	oppos := uint32(0)
	txpos := oppos + int32Size
	fpos := txpos + int32Size
	bpos := fpos + int32Size + uint32(len(blockFilename))
	ofpos := bpos + int32Size
	vpos := ofpos + int32Size
	recLen := vpos + int32Size + uint32(len(lr.value))

	p := storage.NewPage(recLen)

	p.SetUint32(oppos, lr.op)
	p.SetInt32(txpos, lr.txnum)
	p.SetString(fpos, blockFilename)
	p.SetUint32(bpos, lr.block.Number())
	p.SetUint32(ofpos, lr.offset)
	p.SetString(vpos, lr.value)

	return p.Content()
}

func (lr *SetStringLogRecord) unmarshalBytes(rawRecord []byte) error {
	p := storage.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)
	lr.txnum = p.GetInt32(int32Size)

	fpos := uint32(2 * int32Size) //nolint:gomnd
	blockFilename := p.GetString(fpos)

	bpos := fpos + uint32(int32Size+len(blockFilename))
	blockNum := p.GetUint32(bpos)

	lr.block = storage.NewBlockID(blockFilename, blockNum)

	ofpos := bpos + int32Size
	lr.offset = p.GetUint32(ofpos)

	vpos := ofpos + int32Size
	lr.value = p.GetString(vpos)

	return nil
}
