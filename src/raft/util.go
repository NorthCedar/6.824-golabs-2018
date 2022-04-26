package raft

import (
	rand2 "crypto/rand"
	"fmt"
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
	Term 		int
	Index 		int
	Command 	interface{}
}

func newEntry() *Entries {
	res := &Entries{
		entries: make([][]*Entry, 1),
		index:   1,
	}
	res.entries[0] = make([]*Entry, 1)
	res.entries[0][0] = &Entry{
		Term:    0,
		Index:   0,
		Command: nil,
	}
	return res
}

func (es *Entries) getLastEntry() *Entry {
	lastTerm := es.entries[len(es.entries)-1]
	return lastTerm[len(lastTerm)-1]
}

func (es *Entries) getPrevEntry(nowIndex int) (*Entry, []*Entry) {
	lastLog := es.getLastEntry()
	res := make([]*Entry, 0)
	if nowIndex > es.index {
		if nowIndex - es.index > 1 {
			panic("something wrong")
		}
		return lastLog, res
	}

	step := es.index - nowIndex + 1
	for i := lastLog.Term; i>=0; i-- {
		for j := len(es.entries[i])-1; j>=0; j-- {
			if step == 0 {
				return es.entries[i][j], res
			}
			res = append(res, es.entries[i][j])
			step--
		}
	}

	if step == 0 {
		return es.entries[0][0], res
	}
	panic("something wrong" + fmt.Sprintf("es.index %v, nowIndex %v", es.index, nowIndex))
	return nil, nil
}

func (es *Entries)delete(endTerm, endIndex int) {
	lastLog := es.getLastEntry()
	nowTerm := lastLog.Term
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
	nowTerm := lastLog.Term

	for ; nowTerm < e.Term; {
		es.entries = append(es.entries, []*Entry{})
		nowTerm++
	}
	nowTerm = e.Term

	nowIndex := len(es.entries[nowTerm])-1
	for ; nowIndex < e.Index; {
		es.entries[nowTerm] = append(es.entries[nowTerm], &Entry{})
		nowIndex++
		es.index++
	}
	nowIndex = e.Index

	es.entries[nowTerm][nowIndex] = &Entry{
		Term:    e.Term,
		Index:   e.Index,
		Command: e.Command,
	}
}

func (es *Entries) appendOnly(command interface{}, currentTerm int) {
	lastLog := es.getLastEntry()
	nowTerm := lastLog.Term
	for nowTerm < currentTerm  {
		es.entries = append(es.entries, []*Entry{})
		nowTerm++
	}
	nowTerm = currentTerm

	es.entries[nowTerm] = append(es.entries[nowTerm], &Entry{
		Term:    nowTerm,
		Index:   len(es.entries[nowTerm]),
		Command: command,
	})
	es.index++
}

const BeatTimeout = 100 *time.Millisecond
const BasicWaitTimeout = 200 *time.Millisecond
const SendWaitTimeout = 10 * time.Millisecond
func getRandTime(node int) time.Duration {
	rng, _ := rand2.Int(rand2.Reader, big.NewInt(200))
	tmp := time.Duration(rng.Uint64()) * time.Millisecond
	tmp += BasicWaitTimeout
	fmt.Printf("reset timer - %v\n", node)
	return tmp
}
