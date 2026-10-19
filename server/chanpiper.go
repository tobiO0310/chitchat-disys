package main

// Taken from https://github.com/transcelestial/chanpiper/blob/main/v2/chanpiper.go
// And changed to fit our usage.

import (
	"sync"
)

type Message struct {
	message     string
	author_name string
	author_id   uint64
	timestamp   uint64
}

func NewPiper(source <-chan Message) Piper {
	p := &piper{
		chans: []chan<- Message{},
	}
	go p.setupRcvLoop(source)
	return p
}

type Piper interface {
	Pipe() <-chan Message
}

type piper struct {
	mux   sync.RWMutex
	chans []chan<- Message
}

func (p *piper) Pipe() <-chan Message {
	p.mux.Lock()
	defer p.mux.Unlock()
	c := make(chan Message, 1)
	if p.chans == nil {
		return nil
	}
	p.chans = append(p.chans, c)
	return c
}

func (p *piper) setupRcvLoop(source <-chan Message) {
	for d := range source {
		p.mux.RLock()
		for _, c := range p.chans {
			select {
			// don't block
			case c <- d:
			default:
			}
		}
		p.mux.RUnlock()
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	// source got closed, so close the receivers
	for _, c := range p.chans {
		close(c)
	}
	// empty chans
	p.chans = nil
}
