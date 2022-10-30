package utils_test

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/unhandled-exception/sophiadb/internal/pkg/utils"
)

func TestCondLock(t *testing.T) {
	x := 0
	c := utils.NewCond(&sync.Mutex{})
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

func TestCondLock_WaitWithTimeout(t *testing.T) {
	c := utils.NewCond(&sync.Mutex{})
	c.L.Lock()

	hasData := make(chan struct{})
	ch := make(chan int)

	go func(hasData chan<- struct{}) {
		ch <- 1

		time.Sleep(250 * time.Millisecond)

		hasData <- struct{}{}

		c.Broadcast()
	}(hasData)

	// Синхроизируем запуск горутины и ожидания броадкаста
	<-ch

	c.WaitWithTimeout(50 * time.Millisecond)
	select {
	case <-hasData:
		assert.Fail(t, "has data before deadline")
	default:
	}

	c.WaitWithTimeout(1000 * time.Millisecond)
	select {
	case <-hasData:
	default:
		assert.Fail(t, "has not data after deadline")
	}

	// Проверяем пустые вызовы
	c.WaitWithTimeout(20 * time.Millisecond)

	c.L.Unlock()
	c.Broadcast()
}
