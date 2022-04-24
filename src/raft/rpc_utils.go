package raft

func (rf *Raft) appendEntry(args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.entries.delete(args.PrevLogTerm, args.PrevLogIndex)
	for _, e := range args.Entries {
		rf.entries.addEntry(e)
	}
}

func (rf *Raft) initLeader() {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.entries.index+1
	}
}

func (rf *Raft) appendOnly(command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	rf.entries.appendOnly(command, rf.currentTerm)
}
