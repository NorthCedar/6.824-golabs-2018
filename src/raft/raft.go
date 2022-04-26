package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// need persisted
	currentTerm 		int
	votedFor 			int
	entries 			*Entries

	// all servers
	commitIndex 		int
	lastApplied 		int

	// leader
	nextIndex			[]int
	matchIndex			[]int

	endCh 				chan bool
	state 				int
	timer 				*time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == 0
	fmt.Printf("Get %v State = %v, term = %v \n", rf.me, rf.state, rf.currentTerm)

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	fmt.Printf("node %v start with command %v\n", rf.me, command)
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	index := rf.entries.index
	term := rf.currentTerm
	isLeader := rf.state == 0

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	go func(command interface{}) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.appendOnly(command)
		rf.tryCommit()
	}(command)

	return index, term, isLeader
}

func (rf *Raft) tryCommit() {
	if rf.state != 0 {
		return
	}
	readyNodes := 0
	for node, startIndex := range rf.nextIndex {
		if node == rf.me {
			continue
		}
		
		reply := new(AppendEntriesReply)
		prevLog, es := rf.entries.getPrevEntry(startIndex)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      es,
			LeaderCommit: rf.commitIndex,
		}

		ok := rf.sendAppendEntries(node, args, reply)
		if !ok {
			continue
		}

		if reply.Term > rf.currentTerm {
			fmt.Printf("node %v's term(%v) is bigger than leader %v's term(%v)\n", node, reply.Term, rf.me, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.state = 2
			rf.timer.Reset(getRandTime(rf.me))
			return
		}
		if reply.Success {
			readyNodes++
			rf.matchIndex[node] = rf.entries.index
			rf.nextIndex[node] = rf.entries.index+1
		}else if rf.nextIndex[node] > 1 {
			rf.nextIndex[node]--
		}
	}

	if readyNodes >= (len(rf.peers) + 1) / 2 && rf.commitIndex != rf.entries.index {
		last := rf.entries.getLastEntry()
		if last.Term != rf.currentTerm {
			fmt.Printf("leader cannot commit other term log")
			rf.timer.Reset(BeatTimeout)
			return
		}

		rf.commitIndex = rf.entries.index
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      last.Command,
			CommandIndex: rf.commitIndex-1,
		}
		fmt.Printf("tryCommit： leader node %v commit to %v(%v-%v) ok\n", rf.me, rf.commitIndex, last.Term, last.Index)
	}

	rf.timer.Reset(BeatTimeout)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.endCh)
	return
}

func (rf *Raft) election() {
	rf.state = 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	limit := (len(rf.peers) + 1) / 2
	support := 1
	fmt.Printf("node %v start election in term %v\n", rf.me, rf.currentTerm)

	reply := new(RequestVoteReply)
	lastLog := rf.entries.getLastEntry()
	arg := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for node := range rf.peers {
		if node == rf.me {
			continue
		}

		ok := rf.sendRequestVote(node, arg, reply)
		if !ok {
			continue
		}

		if reply.VoteGranted {
			support++
		}
		if reply.Term > rf.currentTerm {
			fmt.Printf("node %v's term(%v) is bigger than candidate %v's term(%v)\n", node, reply.Term, rf.me, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.state = 2
			rf.timer.Reset(getRandTime(rf.me))
			return
		}
		fmt.Printf("node %v(term %v) request node %v, get reply: %v, %v\n", rf.me, rf.currentTerm, node, reply.Term, reply.VoteGranted)
	}
	if support >= limit {
		fmt.Printf("node %v wins in term %v election\n", rf.me, rf.currentTerm)
		rf.state = 0
		rf.initLeader()
		rf.tryCommit()
		return
	}
	rf.state = 2
	rf.timer.Reset(getRandTime(rf.me))
	fmt.Printf("node %v loses in term %v election\n", rf.me, rf.currentTerm)
}

func (rf *Raft) life() {
	for {
		select {
		case <-rf.endCh:
			fmt.Printf("stop service %v\n", rf.me)
			return
		case <-rf.timer.C:
			go rf.handleTimeOut()
		default:

		}

	}
}

func (rf *Raft) handleTimeOut() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	if rf.state == 0 {
		rf.tryCommit()
	}else {
		rf.election()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.entries = newEntry()
	rf.commitIndex = 1
	rf.lastApplied = 1
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.endCh = make(chan bool, 1)
	rf.state = 2
	rf.timer = time.NewTimer(getRandTime(rf.me))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		defer func() {
			if err := recover(); err!= nil {
				fmt.Println("get panic in Make: ", err)
			}
		}()
		rf.life()
	}()

	return rf
}
