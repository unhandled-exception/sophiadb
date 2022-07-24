package types

import (
	"encoding/binary"
	"math"
)

const (
	BoolSize           = 1
	BoolTrueMark  byte = 0xf0
	BoolFalseMark byte = 0x0f
	Int32Size          = 4
	Int64Size          = 8
)

// Page – страница базы в памяти
type Page struct {
	bb    []byte
	order binary.ByteOrder
}

// PageStringBytesLen возвращает предельный размер строки на странице в байтах
func PageStringBytesLen(length int) uint32 {
	return uint32(Int32Size + length*4) // длина строки + 4 байта на символ в utf-8
}

// PageStringBytesLen возвращает предельный размер строки на странице в байтах
func PageInt64BytesLen() uint32 {
	return Int64Size
}

// NewPage создает новую страницу в памяти размером size байт
func NewPage(size uint32) *Page {
	return &Page{
		bb:    make([]byte, size),
		order: binary.LittleEndian,
	}
}

func NewPageFromBytes(rawPage []byte) *Page {
	return &Page{
		bb:    rawPage,
		order: binary.LittleEndian,
	}
}

// Len возвращает размер страницы в байтах
func (p *Page) Len() uint32 {
	return uint32(len(p.bb))
}

// Content возвращает содержимое страницы в виде массива байтов
func (p *Page) Content() []byte {
	return p.bb
}

// PutBytes записывает массив байтов по смещению в страницу
func (p *Page) PutBytes(offset uint32, value []byte) {
	for i, v := range value {
		p.bb[int(offset)+i] = v
	}
}

// FetchBytes возвращает массив байтов из страницы по смещению offset длинной size
func (p *Page) FetchBytes(offset uint32, size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = p.bb[int(offset)+i]
	}

	return result
}

// GetInt32 возвращает значение int32 по смещению offset
func (p *Page) GetInt32(offset uint32) int32 {
	buf := p.FetchBytes(offset, Int32Size)
	value := int32(p.order.Uint32(buf))

	return value
}

// SetInt32 записывает значение int32 по смещению offset
func (p *Page) SetInt32(offset uint32, value int32) {
	buf := make([]byte, Int32Size)
	p.order.PutUint32(buf, uint32(value))
	p.PutBytes(offset, buf)
}

// GetUint32 возвращает значение uint32 по смещению offset
func (p *Page) GetUint32(offset uint32) uint32 {
	return uint32(p.GetInt32(offset))
}

// SetUint32 записывает значение uint32 по смещению offset
func (p *Page) SetUint32(offset uint32, value uint32) {
	p.SetInt32(offset, int32(value))
}

// GetInt64 возвращает значение int64 по смещению offset
func (p *Page) GetInt64(offset uint32) int64 {
	buf := p.FetchBytes(offset, Int64Size)
	value := int64(p.order.Uint64(buf))

	return value
}

// SetInt64 записывает значение int64 по смещению offset
func (p *Page) SetInt64(offset uint32, value int64) {
	buf := make([]byte, Int64Size)
	p.order.PutUint64(buf, uint64(value))
	p.PutBytes(offset, buf)
}

// GetBytes возвращает байтовый массив по смещению offset
func (p *Page) GetBytes(offset uint32) []byte {
	length := p.GetInt32(offset)

	return p.FetchBytes(offset+Int32Size, int(length))
}

// SetBytes записывает байтовый массив по смещению offset
func (p *Page) SetBytes(offset uint32, value []byte) {
	length := len(value)
	p.SetInt32(offset, int32(length))
	p.PutBytes(offset+Int32Size, value)
}

// GetString возвращает строку по смещению offset
func (p *Page) GetString(offset uint32) string {
	return string(p.GetBytes(offset))
}

// SetString записывает строку по смещению offset
func (p *Page) SetString(offset uint32, value string) {
	p.SetBytes(offset, []byte(value))
}

// GetFloat32 возвращает значение float32 по смещению offset
func (p *Page) GetFloat32(offset uint32) float32 {
	buf := p.FetchBytes(offset, Int32Size)
	value := math.Float32frombits(p.order.Uint32(buf))

	return value
}

// SetFloat32 записывает значение float32 по смещению offset
func (p *Page) SetFloat32(offset uint32, value float32) {
	buf := make([]byte, Int32Size)
	p.order.PutUint32(buf, math.Float32bits(value))
	p.PutBytes(offset, buf)
}

// GetBool возвращает значение bool по смещению offset
func (p *Page) GetBool(offset uint32) bool {
	return (p.bb[offset] == BoolTrueMark)
}

// SetBool записывает значение bool по смещению offset
func (p *Page) SetBool(offset uint32, value bool) {
	bValue := BoolFalseMark
	if value {
		bValue = BoolTrueMark
	}

	p.bb[offset] = bValue
}
