package utils

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCond(t *testing.T) {
	x := 0
	c := NewCond(&sync.Mutex{})
	done := make(chan bool)
	go func() {
		c.L.Lock()
		x = 1
		c.Wait()
		if x != 2 {
			log.Fatal("want 2")
		}
		x = 3
		c.Broadcast()
		c.L.Unlock()
		done <- true
	}()
	go func() {
		c.L.Lock()
		for {
			if x == 1 {
				x = 2
				c.Broadcast()
				break
			}
			c.L.Unlock()
			runtime.Gosched()
			c.L.Lock()
		}
		c.L.Unlock()
		done <- true
	}()
	go func() {
		c.L.Lock()
		for {
			if x == 2 {
				c.Wait()
				if x != 3 {
					log.Fatal("want 3")
				}
				break
			}
			if x == 3 {
				break
			}
			c.L.Unlock()
			runtime.Gosched()
			c.L.Lock()
		}
		c.L.Unlock()
		done <- true
	}()
	<-done
	<-done
	<-done
}

func TestWaitWithTimeout(t *testing.T) {
	c := NewCond(&sync.Mutex{})
	c.L.Lock()

	hasData := false
	ch := make(chan int)

	go func() {
		ch <- 1
		time.Sleep(250 * time.Millisecond)
		hasData = true
		c.Broadcast()
	}()

	// Синхроизируем запуск горутины и ожидания броадкаста
	<-ch

	c.WaitWithTimeout(50 * time.Millisecond)
	assert.False(t, hasData)

	c.WaitWithTimeout(1000 * time.Millisecond)
	assert.True(t, hasData)

	// Проверяем пустые вызовы
	c.WaitWithTimeout(20 * time.Millisecond)

	c.L.Unlock()
	c.Broadcast()
}
