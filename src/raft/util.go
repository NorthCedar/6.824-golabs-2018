package raft

import (
	rand2 "crypto/rand"
	"log"
	"math/big"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Entries struct {
	entries 	[][]*Entry
	index 		int
}

type Entry struct {
	term 		int
	index 		int
	command 	interface{}
}

func newEntry() *Entries {
	res := &Entries{
		entries: make([][]*Entry, 1),
		index:   1,
	}
	res.entries[0] = make([]*Entry, 1)
	res.entries[0][0] = &Entry{
		term:    0,
		index:   0,
		command: nil,
	}
	return res
}

func (es *Entries) getLastEntry() *Entry {
	lastTerm := es.entries[len(es.entries)-1]
	return lastTerm[len(lastTerm)-1]
}

func (es *Entries) getPrevEntry(nowIndex int) (*Entry, []*Entry) {
	if nowIndex > es.index {
		panic("something wrong")
	}
	res := make([]*Entry, 0)
	step := es.index - nowIndex + 1
	lastLog := es.getLastEntry()
	for i := lastLog.term; i>=0; i-- {
		for j := len(es.entries[i])-1; j>=0; j-- {
			if step == 0 {
				return es.entries[i][j], res
			}
			res = append(res, es.entries[i][j])
			step--
		}
	}

	panic("something wrong")
	return nil, nil
}

func (es *Entries)delete(endTerm, endIndex int) {
	lastLog := es.getLastEntry()
	nowTerm := lastLog.term
	for nowTerm > endTerm {
		es.index -= len(es.entries[nowTerm])
		es.entries = es.entries[:nowTerm]
		nowTerm--
	}
	nowTerm = endTerm

	nowIndex := len(es.entries[nowTerm])-1
	for nowIndex > endIndex {
		es.entries[nowTerm] = es.entries[nowTerm][:nowIndex]
		nowIndex--
		es.index--
	}
}

func (es *Entries)addEntry(e *Entry) {
	lastLog := es.getLastEntry()
	nowTerm := lastLog.term

	for ; nowTerm < e.term; {
		es.entries = append(es.entries, []*Entry{})
		nowTerm++
	}
	nowTerm = e.term

	nowIndex := len(es.entries[nowTerm])-1
	for ; nowIndex < e.index; {
		es.entries[nowTerm] = append(es.entries[nowTerm], &Entry{})
		nowIndex++
		es.index++
	}
	nowIndex = e.index

	es.entries[nowTerm][nowIndex] = &Entry{
		term:    e.term,
		index:   e.index,
		command: e.command,
	}
}

func (es *Entries) appendOnly(command interface{}, currentTerm int) {
	lastLog := es.getLastEntry()
	nowTerm := lastLog.term
	for nowTerm < currentTerm  {
		es.entries = append(es.entries, []*Entry{})
		nowTerm++
	}
	nowTerm = currentTerm

	es.entries[nowTerm] = append(es.entries[nowTerm], &Entry{
		term:    nowTerm,
		index:   len(es.entries[nowTerm]),
		command: command,
	})
	es.index++
}

const beatTime = 200
const SendWaitTimeout = 100 * time.Millisecond
func getRandTime(i int) time.Duration {
	rng, _ := rand2.Int(rand2.Reader, big.NewInt(200))
	tmp := uint32(rng.Uint64())
	tmp += beatTime
	tmp += uint32(i)
	return time.Duration(tmp) * time.Millisecond
}
