package types

import "fmt"

type SlotID int32

type RID struct {
	BlockNumber int32
	Slot        SlotID
}

func (r RID) String() string {
	return fmt.Sprintf("RID[%d, %d]", r.BlockNumber, r.Slot)
}

func (r RID) Equals(another RID) bool {
	return r.BlockNumber == another.BlockNumber && r.Slot == another.Slot
}
