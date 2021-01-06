package storage

import (
	"encoding/binary"
	"math"
)

const (
	boolTrueMark  byte = 0xf0
	boolFalseMark byte = 0x0f
)

// Page – страница базы в памяти
type Page struct {
	bb    []byte
	order binary.ByteOrder
}

// NewPage создает новую страницу в памяти размером size байт
func NewPage(size int) *Page {
	return &Page{
		bb:    make([]byte, size),
		order: binary.LittleEndian,
	}
}

// Len возвращает размер страницы в байтах
func (p *Page) Len() int {
	return len(p.bb)
}

// Content возвращает содержимое страницы в виде массива байтов
func (p *Page) Content() []byte {
	return p.bb
}

// putBytes записывает массив байтов по смещению в страницу
func (p *Page) putBytes(offset int, value []byte) {
	for i, v := range value {
		p.bb[offset+i] = v
	}
}

// fetchBytes возвращает массив байтов из страницы по смещению offset длинной size
func (p *Page) fetchBytes(offset int, size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = p.bb[offset+i]
	}
	return result
}

// GetInt32 возвращает значение int32 по смещению offset
func (p *Page) GetInt32(offset int) int32 {
	buf := p.fetchBytes(offset, 4)
	value := int32(p.order.Uint32(buf))
	return value
}

// SetInt32 записывает значение int32 по смещению offset
func (p *Page) SetInt32(offset int, value int32) {
	buf := make([]byte, 4)
	p.order.PutUint32(buf, uint32(value))
	p.putBytes(offset, buf)
}

// GetInt64 возвращает значение int64 по смещению offset
func (p *Page) GetInt64(offset int) int64 {
	buf := p.fetchBytes(offset, 8)
	value := int64(p.order.Uint64(buf))
	return value
}

// SetInt64 записывает значение int64 по смещению offset
func (p *Page) SetInt64(offset int, value int64) {
	buf := make([]byte, 8)
	p.order.PutUint64(buf, uint64(value))
	p.putBytes(offset, buf)
}

// GetBytes возвращает байтовый массив по смещению offset
func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt32(offset)
	return p.fetchBytes(offset+4, int(length))
}

// SetBytes записывает байтовый массив по смещению offset
func (p *Page) SetBytes(offset int, value []byte) {
	length := len(value)
	p.SetInt32(offset, int32(length))
	p.putBytes(offset+4, value)
}

// GetString возвращает строку по смещению offset
func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

// SetString записывает строку по смещению offset
func (p *Page) SetString(offset int, value string) {
	p.SetBytes(offset, []byte(value))
}

// GetFloat32 возвращает значение float32 по смещению offset
func (p *Page) GetFloat32(offset int) float32 {
	buf := p.fetchBytes(offset, 4)
	value := math.Float32frombits(p.order.Uint32(buf))
	return value
}

// SetFloat32 записывает значение float32 по смещению offset
func (p *Page) SetFloat32(offset int, value float32) {
	buf := make([]byte, 4)
	p.order.PutUint32(buf, math.Float32bits(value))
	p.putBytes(offset, buf)
}

// GetBool возвращает значение bool по смещению offset
func (p *Page) GetBool(offset int) bool {
	return (p.bb[offset] == boolTrueMark)
}

// SetBool записывает значение bool по смещению offset
func (p *Page) SetBool(offset int, value bool) {
	var bValue byte = boolFalseMark
	if value {
		bValue = boolTrueMark
	}
	p.bb[offset] = bValue
}
