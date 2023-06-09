package raft

import (
	"fmt"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	fmt.Printf("node %v handle node %v's RequestVote: currentTerm %v, candidateTerm %v\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Printf("node %v(%v) handle node %v(%v)'s RequestVote fail: check term error\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	rf.currentTerm = args.Term
	lastLog := rf.entries.getLastEntry()
	lastLogTerm := lastLog.Term
	lastLogIndex := lastLog.Index
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex){
		fmt.Printf("node %v(%v) handle node %v(%v)'s RequestVote fail: check log error: self(%v-%v) candidate(%v-%v)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term, lastLogTerm, lastLogIndex, args.LastLogTerm, args.LastLogIndex)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.state = 2
	rf.timer.Reset(getRandTime(rf.me))
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	fmt.Printf("node %v(%v) handle node %v(%v)'s RequestVote ok\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	t := time.After(SendWaitTimeout)
	isSend := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		isSend <- ok
	}()
	var ok bool
	select {
	case ok = <-isSend:
		if !ok {
			fmt.Printf("node %v(term %v) request node %v, get reply fail\n", rf.me, rf.currentTerm, server)
		}
		break
	case <- t:
		fmt.Printf("node %v sendRequestVote to node %v timeout in term %v\n", rf.me, server, rf.currentTerm)
	}

	return ok
}


type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm 	int
	Entries			[]*Entry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	t := time.After(SendWaitTimeout)
	isSend := make(chan bool, 1)
	var ok bool
	go func() {
		isOk := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		isSend <- isOk
	}()
	select {
	case ok = <-isSend:
		if !ok {
			//fmt.Printf("node %v(term %v) request node %v, get reply fail\n", rf.me, rf.currentTerm, server)
		}
		break
	case <- t:
		//fmt.Printf("node %v sendAppendEntries to node %v timeout in term %v\n", rf.me, server, rf.currentTerm)
		ok = false
	}

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	checkState := func() bool {
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			return false
		}
		rf.currentTerm = args.Term

		rf.votedFor = args.LeaderId
		reply.Term = rf.currentTerm
		rf.state = 2
		rf.timer.Reset(getRandTime(rf.me))
		return true
	}
	checkLog := func() bool {
		isExist, _ := rf.entries.findEntry(args.PrevLogTerm, args.PrevLogIndex)
		return isExist
	}

	reply.Success = checkState() && checkLog()
	if !reply.Success {
		return
	}

	rf.appendEntry(args)
	if rf.commitIndex != args.LeaderCommit {
		fmt.Printf("node %v(%v) follows leader %v(%v) commit to %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LeaderCommit)
		//rf.entries.getPrevEntry(args.LeaderCommit)
	}else {
		fmt.Printf("node %v(%v) receives leader %v(%v)'s beat\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}

	go func(prev, now int, am chan ApplyMsg) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.entries.addToApplyCh(prev, now, am)
	}(rf.commitIndex, args.LeaderCommit, rf.applyCh)

	rf.commitIndex = args.LeaderCommit
	return
}
