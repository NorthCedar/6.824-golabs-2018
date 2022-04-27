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
	entries 	[]*Entry
	cap 		int
}

type Entry struct {
	Term 		int
	Index 		int
	Command 	interface{}
}

func newEntry() *Entries {
	res := &Entries{
		entries: make([]*Entry, 1),
		cap:   1,
	}
	res.entries[0] = &Entry{
		Term:    0,
		Index:   0,
		Command: nil,
	}
	return res
}

func (es *Entries) getLastEntry() *Entry {
	return es.entries[es.cap-1]
}

func (es *Entries) addToApplyCh(prevCommit, nowCommit int, applyCh chan ApplyMsg) {
	//fmt.Printf("addToApplyCh prevCommit %v, nowCommit %v\n", prevCommit, nowCommit)
	for i:= prevCommit+1; i <= nowCommit; i++ {
		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      es.entries[i].Command,
			CommandIndex: i,
		}
		//fmt.Printf("apply %v\n", es.entries[i].Command)
	}
}

func (es *Entries) getPrevEntry(nowIndex int) (*Entry, []*Entry) {
	if nowIndex-1 >= es.cap || nowIndex == 0 {
		panic("something wrong")
	}
	return es.entries[nowIndex-1], es.entries[nowIndex:es.cap]
}

func (es *Entries)delete(endTerm, endIndex int) {
	lastLog := es.getLastEntry()
	for lastLog.Term > endTerm || (lastLog.Term == endTerm && lastLog.Index > endIndex) {
		es.cap--
		lastLog = es.getLastEntry()
	}
}

func (es *Entries)findEntry(term, index int) (bool, int) {
	lastLog := es.getLastEntry()
	now := es.cap-1
	for lastLog.Term != term || lastLog.Index != index {
		if now == 0 || lastLog.Term < term{
			return false, 0
		}
		now--
		lastLog = es.entries[now]
	}
	return true, now
}

func (es *Entries)addEntry(e *Entry) {
	if len(es.entries) <= es.cap {
		es.entries = append(es.entries, make([]*Entry, es.cap)...)
	}
	es.entries[es.cap] = e
	es.cap++
}

func (es *Entries) appendOnly(command interface{}, currentTerm int) {
	lastLog := es.getLastEntry()
	nowIndex := 0
	if lastLog.Term == currentTerm {
		nowIndex = lastLog.Index+1
	}
	e := &Entry{
		Term:    currentTerm,
		Index:   nowIndex,
		Command: command,
	}
	es.addEntry(e)
}

const BeatTimeout = 100 *time.Millisecond
const BasicWaitTimeout = 300 *time.Millisecond
const SendWaitTimeout = 30 * time.Millisecond
func getRandTime(node int) time.Duration {
	rng, _ := rand2.Int(rand2.Reader, big.NewInt(200))
	tmp := time.Duration(rng.Uint64()) * time.Millisecond
	tmp += BasicWaitTimeout
	fmt.Printf("reset timer - %v\n", node)
	return tmp
}
