package storage

import (
	"encoding/binary"
)

// Page – страница базы в памяти
type Page struct {
	bb []byte
}

// NewPage создает новую страницу в памяти размером size байт
func NewPage(size int64) *Page {
	return &Page{
		bb: make([]byte, size),
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

// GetInt возвращает значение int64 по смещению offset
func (p *Page) GetInt(offset int) int64 {
	buf := p.fetchBytes(offset, 8)
	value, _ := binary.Varint(buf)
	return value
}

// SetInt записывает значение int64 по смещению offset
func (p *Page) SetInt(offset int, value int64) {
	buf := make([]byte, 8)
	binary.PutVarint(buf, value)
	p.putBytes(offset, buf)
}

// GetBytes возвращает байтовый массив по смещению offset
func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.fetchBytes(offset+8, int(length))
}

// SetBytes записывает байтовый массив по смещению offset
func (p *Page) SetBytes(offset int, value []byte) {
	length := len(value)
	p.SetInt(offset, int64(length))
	p.putBytes(offset+8, value)
}

// GetString возвращает строку по смещению offset
func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

// SetString записывает строку по смещению offset
func (p *Page) SetString(offset int, value string) {
	p.SetBytes(offset, []byte(value))
}
