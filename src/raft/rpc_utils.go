package raft

import "fmt"

func (rf *Raft) appendEntry(args *AppendEntriesArgs) {
	last := rf.entries.getLastEntry()
	fmt.Printf("%v before appendEntry: %v-%v\n", rf.me, last.Term, last.Index)

	rf.entries.delete(args.PrevLogTerm, args.PrevLogIndex)

	for _, e := range args.Entries {
		last = rf.entries.getLastEntry()
		if last.Term > e.Term || (last.Term == e.Term && last.Index >= e.Index) {
			panic("invalid entries")
		}
		rf.entries.addEntry(e)
	}

	last = rf.entries.getLastEntry()
	fmt.Printf("%v after appendEntry: %v-%v\n", rf.me, last.Term, last.Index)
}

func (rf *Raft) initLeader() {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.entries.cap
	}
}

func (rf *Raft) appendOnly(command interface{}) {
	rf.entries.appendOnly(command, rf.currentTerm)
}
