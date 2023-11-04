package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BTreeLeaf struct {
	trx         scan.TRXInt
	layout      records.Layout
	searchKey   scan.Constant
	contents    *BTreePage
	currentSlot types.SlotID
	filename    string
}

func NewBTreeLeaf(trx scan.TRXInt, block types.Block, layout records.Layout, searchKey scan.Constant) (*BTreeLeaf, error) {
	contents, err := NewBTreePage(trx, block, layout)
	if err != nil {
		return nil, err
	}

	leaf := &BTreeLeaf{
		trx:         trx,
		layout:      layout,
		searchKey:   searchKey,
		filename:    block.Filename,
		contents:    contents,
		currentSlot: -1,
	}

	if err = leaf.ResetSlot(); err != nil {
		return nil, err
	}

	return leaf, nil
}

func (l *BTreeLeaf) CurrentSlot() types.SlotID {
	return l.currentSlot
}

func (l *BTreeLeaf) ResetSlot() error {
	currentSlot, err := l.contents.FindSlotBefore(l.searchKey)
	if err != nil {
		return err
	}

	l.currentSlot = currentSlot

	return nil
}

func (l *BTreeLeaf) Close() {
	l.contents.Close()
}

func (l *BTreeLeaf) RID() (types.RID, error) {
	return l.contents.GetDataRID(l.currentSlot)
}

func (l *BTreeLeaf) Next() (bool, error) {
	l.currentSlot++

	records, err := l.contents.GetRecords()
	if err != nil {
		return false, err
	}

	if int64(l.currentSlot) >= records {
		return l.tryOverflow()
	}

	dataval, err1 := l.contents.GetVal(l.currentSlot)
	if err1 != nil {
		return false, err1
	}

	if dataval.CompareTo(l.searchKey) == scan.CompEqual {
		return true, nil
	}

	return l.tryOverflow()
}

func (l *BTreeLeaf) Delete(dataRID types.RID) (bool, error) {
	for {
		if ok, err := l.Next(); !ok || err != nil {
			return false, err
		}

		dr, err := l.RID()
		if err != nil {
			return false, err
		}

		if dr.Equals(dataRID) {
			if err1 := l.contents.Delete(l.currentSlot); err1 != nil {
				return false, err
			}

			return true, nil
		}
	}
}

func (l *BTreeLeaf) Insert(dataRID types.RID) (*BTreeDirEntry, error) {
	flag, err := l.contents.GetFlag()
	if err != nil {
		return nil, err
	}

	firstKey, err := l.contents.GetVal(0)
	if err != nil {
		return nil, err
	}

	// Если блок ссылается на блок переполнения и ключ меньше первого в блоке,
	// то текущий блок целиком отщепляем в новый и возвращаем каталожную запись для нового блока
	// dataRID вставляем в текущую страницу
	if flag >= 0 && firstKey.CompareTo(l.searchKey) == scan.CompGreat {
		newBlock, err1 := l.contents.Split(0, flag)
		if err1 != nil {
			return nil, err1
		}

		if err1 = l.contents.SetFlag(BTreeNewFlag); err1 != nil {
			return nil, err1
		}

		l.currentSlot = 0

		if err1 = l.contents.InsertLeaf(l.currentSlot, l.searchKey, dataRID); err1 != nil {
			return nil, err1
		}

		return &BTreeDirEntry{
			BlockNumber: newBlock.Number,
			Dataval:     l.searchKey,
		}, nil
	}

	l.currentSlot++

	if err1 := l.contents.InsertLeaf(l.currentSlot, l.searchKey, dataRID); err1 != nil {
		return nil, err
	}

	full, err := l.contents.IsFull()
	if err != nil {
		return nil, err
	}

	if !full {
		return nil, nil //nolint:nilnil
	}

	// Если в блоке нет места, то расщепляем
	records, err := l.contents.GetRecords()
	if err != nil {
		return nil, err
	}

	lastKey, err := l.contents.GetVal(types.SlotID(records - 1))
	if err != nil {
		return nil, err
	}

	// Если блок содержит одно значение во всех слотах, то создать блок переполнения и перенести в него все записи кроме первой
	if lastKey.CompareTo(firstKey) == scan.CompEqual {
		newBlock, err1 := l.contents.Split(1, flag)
		if err1 != nil {
			return nil, err1
		}

		if err1 = l.contents.SetFlag(int64(newBlock.Number)); err1 != nil {
			return nil, err1
		}

		return nil, nil //nolint:nilnil
	}

	// Расщепляем блок
	splitPos := types.SlotID(records / 2) //nolint:gomnd

	splitKey, err := l.contents.GetVal(splitPos)
	if err != nil {
		return nil, err
	}

	// Если средний ключи равен первому, то двигаемся вправо до нового ключа
	if splitKey.CompareTo(firstKey) == scan.CompEqual { //nolint:nestif
		for {
			splitPos++

			newKey, err1 := l.contents.GetVal(splitPos)
			if err1 != nil {
				return nil, err1
			}

			if newKey.CompareTo(splitKey) != scan.CompEqual {
				splitKey = newKey

				break
			}
		}
	} else {
		// Иначе двигаемся влево до первой записи с текущим ключем
		for {
			newKey, err1 := l.contents.GetVal(splitPos - 1)
			if err1 != nil {
				return nil, err1
			}

			if newKey.CompareTo(splitKey) != scan.CompEqual {
				break
			}

			splitPos--
		}
	}

	newBlock, err := l.contents.Split(splitPos, BTreeNewFlag)
	if err != nil {
		return nil, err
	}

	return &BTreeDirEntry{
		BlockNumber: newBlock.Number,
		Dataval:     splitKey,
	}, nil
}

func (l *BTreeLeaf) tryOverflow() (bool, error) {
	firstKey, err := l.contents.GetVal(0)
	if err != nil {
		return false, err
	}

	flag, err := l.contents.GetFlag()
	if err != nil {
		return false, err
	}

	if l.searchKey.CompareTo(firstKey) != scan.CompEqual || flag < 0 {
		return false, nil
	}

	l.contents.Close()

	newBlock := types.Block{
		Filename: l.filename,
		Number:   types.BlockID(flag),
	}

	newContents, err := NewBTreePage(l.trx, newBlock, l.layout)
	if err != nil {
		return false, err
	}

	l.contents = newContents
	l.currentSlot = 0

	return true, nil
}
