package transaction

type BuffersList struct {
	bm buffersManager
}

func NewBuffersList(bm buffersManager) *BuffersList {
	return &BuffersList{
		bm: bm,
	}
}
