package raft

import "fmt"

func (rf *Raft) appendEntry(args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	last := rf.entries.getLastEntry()
	fmt.Printf("%v before appendEntry: %v-%v\n", rf.me, last.Term, last.Index)
	rf.entries.delete(args.PrevLogTerm, args.PrevLogIndex)
	for _, e := range args.Entries {
		rf.entries.addEntry(e)
	}
	last = rf.entries.getLastEntry()
	fmt.Printf("%v after appendEntry: %v-%v\n", rf.me, last.Term, last.Index)
}

func (rf *Raft) initLeader() {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 1
		rf.nextIndex[i] = rf.entries.index+1
	}
}

func (rf *Raft) appendOnly(command interface{}) {
	rf.entries.appendOnly(command, rf.currentTerm)
}
