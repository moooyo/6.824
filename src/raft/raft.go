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
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
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

func __LOG(str string) {
	fmt.Println("[INFO]" + str)
}

func (rf *Raft) INFO_LOG(str string) {
		if !rf.status {
			return
		}
		tmp := fmt.Sprintf("[%d][%d][%d] %s ",
			rf.me, rf.currentTerm, rf.leaderID, str)
		__LOG(tmp)
}

type LogEntry struct {
	Term   int
	Index  int
	Buffer interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	VoteForNull     = -1
	InvalidLeader   = -1
	HeartBeatsIndex = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// All server maintain this state
	locked         int32 // atomic to check is locked
	currentTerm    int
	voteFor        int
	logs           []LogEntry
	logIndex       int
	commitIndex    int
	commitLogIndex int
	lastApplied    int
	leaderID       int
	status         bool
	identity       int
	heartBeatsID   int32
	applyChan      chan ApplyMsg

	// leader maintains
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.identity == Leader
	rf.mu.Unlock()

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.voteFor) != nil ||
		e.Encode(rf.logIndex) != nil || e.Encode(rf.logs) != nil ||
		e.Encode(rf.commitIndex) != nil || e.Encode(rf.commitLogIndex) != nil {
		fmt.Printf("err to save")
	}
	rf.INFO_LOG(fmt.Sprintf("save %d %d %d %d %v", rf.currentTerm, rf.voteFor, rf.logIndex, rf.commitIndex, rf.logs))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logIndex int
	var logs []LogEntry
	var voteFor int
	var currentTerm int
	var commitIndex int
	var commitLogIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&logIndex) != nil || d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil || d.Decode(&commitLogIndex) != nil {
		log.Fatal("restart err")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.commitLogIndex = commitLogIndex
		rf.voteFor = voteFor
		rf.logIndex = logIndex
		rf.logs = logs
		rf.commitIndex = commitIndex
		rf.INFO_LOG(fmt.Sprintf("reload %d %d %d %d %v", rf.currentTerm, rf.voteFor, rf.logIndex, rf.commitIndex, rf.logs))
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		rf.voteFor = VoteForNull
		rf.leaderID = InvalidLeader
		rf.currentTerm = args.Term
	}

	rf.INFO_LOG(fmt.Sprintf("recive vote request from %d", args.CandidateID))

	if rf.voteFor != VoteForNull && rf.voteFor != args.CandidateID {
		// vote false
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// todo
	// check whether it correct
	// restrict 5.4.1
	lastLogTerm := rf.logs[rf.logIndex].Term
	lastLogIndex := rf.logs[rf.logIndex].Index
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm &&
		args.LastLogIndex < lastLogIndex) {
		//vote false
		reply.Term = rf.currentTerm
		rf.INFO_LOG(fmt.Sprintf("vote to %d false args.lastTerm %d args.lastIndex %d lastTerm %d lastIndex %d",
			args.CandidateID, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex))
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// if is a new term. vote
	rf.INFO_LOG(fmt.Sprintf("vote for %d args.term %d", args.CandidateID, args.Term))
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.voteFor = args.CandidateID
	rf.persist()
	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.identity = Follower
		rf.voteFor = VoteForNull
	}
	//
	// restrict:
	//
	lastTerm := rf.logs[rf.commitIndex].Term
	lastIndex := rf.logs[rf.commitIndex].Index
	if lastTerm > args.PreLogTerm {
		rf.INFO_LOG(fmt.Sprintf("out-of-date heartbeats from %d args.preTerm  %d lastTerm %d",
			args.LeaderID, args.PreLogTerm, lastTerm))
		reply.Success = false
		reply.Term = lastTerm
		rf.mu.Unlock()
		return
	}

	rf.leaderID = args.LeaderID
	rf.identity = Follower
	atomic.AddInt32(&rf.heartBeatsID, 1)
	rf.INFO_LOG(fmt.Sprintf("heartbeats from %d", args.LeaderID))
	rf.INFO_LOG(fmt.Sprintf("now logIndex is %d args.term %d args.index %d leadercommit %d",
		rf.logIndex,args.PreLogTerm, args.PreLogIndex, args.LeaderCommit))
	lastTerm = rf.logs[rf.logIndex].Term
	lastIndex = rf.logs[rf.logIndex].Index
	if lastTerm != args.PreLogTerm || lastIndex != args.PreLogIndex {
		//
		// check whether follower have
		// bad log. Because it win election
		// get log from client but not commit
		//
		if lastTerm > args.PreLogTerm || (lastTerm == args.PreLogTerm && lastIndex > args.PreLogIndex) {
			//
			// bad log must be override
			//
			for lastTerm > args.PreLogTerm || (lastTerm == args.PreLogTerm && lastIndex > args.PreLogIndex) {
				rf.logIndex--
				lastTerm = rf.logs[rf.logIndex].Term
				lastIndex = rf.logs[rf.logIndex].Index
			}
			rf.logs = rf.logs[:rf.logIndex + 1]
			reply.Success = false
			reply.Term = rf.logs[rf.logIndex].Term
			rf.INFO_LOG(fmt.Sprintf("bad log now log index is %d cmd is %d",
				rf.logIndex, rf.logs[rf.logIndex].Buffer))
			rf.persist()
		} else {
			//
			// have something more need to append
			//
			reply.Success = false
			reply.Term = lastTerm
			rf.INFO_LOG(fmt.Sprintf("append false lastTerm %d lastIndex %d argsPreTerm %d argsPreIndex %d leadercommit %d",
				lastTerm, lastIndex, args.PreLogTerm, args.PreLogIndex, args.LeaderCommit))
			rf.mu.Unlock()
			return
		}
	}
	if !(args.PreLogTerm == lastTerm && args.PreLogIndex == lastIndex) {
		reply.Success = false
		reply.Term = lastTerm
		rf.mu.Unlock()
		return
	}
	for _, entries := range args.Entries {
		rf.logIndex++
		rf.logs = append(rf.logs, entries)
	}
	reply.Success = true
	reply.Term = lastTerm
	if args.LeaderCommit > rf.commitLogIndex {
		for i := 0; i < len(rf.logs); i++ {
			if rf.logs[i].Index > rf.commitLogIndex && rf.logs[i].Index <= args.LeaderCommit {
				if rf.logs[i].Index != HeartBeatsIndex {
					msg := ApplyMsg{
						CommandValid: rf.logs[i].Buffer != nil,
						Command:      rf.logs[i].Buffer,
						CommandIndex: rf.logs[i].Index,
					}
					if msg.CommandValid {
						rf.INFO_LOG(fmt.Sprintf("follower send %d i is %d", msg.Command.(int), i))
						rf.applyChan <- msg
					}
					rf.commitLogIndex = rf.logs[i].Index
				}
				rf.commitIndex = i
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return
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
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.identity == Leader
	if isLeader {
		log := LogEntry{
			Term:   rf.currentTerm,
			Index:  0,
			Buffer: command,
		}
		for i := rf.logIndex; i >= 0; i-- {
			if rf.logs[i].Index != HeartBeatsIndex {
				log.Index = rf.logs[i].Index + 1
				break
			}
		}
		rf.logs = append(rf.logs, log)
		rf.logIndex++
		if isLeader {
			rf.INFO_LOG(fmt.Sprintf("Start %d", command))
		}
		index = rf.logs[rf.logIndex].Index
		term = rf.logs[rf.logIndex].Term
	}
	if isLeader {
		rf.persist()
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

const (
	HeartBeatsInterval         = 100
	ElectTimeOutInterval       = 250
	ElectTimeOutSleepTimeStart = 1000
)

func ElectionMain(rf *Raft) {
	for {
		time.Sleep(time.Millisecond *
			time.Duration(ElectTimeOutSleepTimeStart+rand.Intn(ElectTimeOutInterval)))
		rf.mu.Lock()
		if rf.identity == Leader {
			rf.mu.Unlock()
			return
		}
		if rf.leaderID != InvalidLeader || rf.identity != Follower || rf.voteFor != VoteForNull {
			rf.identity = Follower
			rf.voteFor = VoteForNull
			rf.mu.Unlock()
			return
		}
		rf.INFO_LOG("start elect")
		rf.identity = Candidate
		rf.currentTerm = rf.currentTerm + 1
		rf.voteFor = rf.me
		rf.leaderID = rf.me
		ac := make(chan bool, len(rf.peers))
		done := make(chan struct{})
		tm := make(chan int)
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.logs[rf.logIndex].Index,
			LastLogTerm:  rf.logs[rf.logIndex].Term,
		}
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			reply := RequestVoteReply{
				Term:        0,
				VoteGranted: false,
			}
			go func(server int, term int, ac chan bool) {
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok && reply.VoteGranted {
					ac <- true
				} else {
					ac <- false
					tm <- reply.Term
				}
			}(index, rf.currentTerm, ac)
		}
		sz := len(rf.peers)
		go func(ac chan bool, sz, term int) {
			accept := 1
			reject := 0
			maxTerm := term
			for {
				ok := <-ac
				if ok {
					accept++
				} else {
					reject++
					t := <-tm
					if t > maxTerm {
						maxTerm = t
					}
				}
				if accept > sz/2 {
					rf.mu.Lock()
					if rf.currentTerm < maxTerm {
						rf.currentTerm = maxTerm
						rf.voteFor = VoteForNull
						rf.leaderID = InvalidLeader
						rf.mu.Unlock()
						return
					}
					rf.INFO_LOG("elect win")
					rf.leaderID = rf.me
					rf.identity = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for index, _ := range rf.peers {
						rf.nextIndex[index] = rf.logIndex + 1
						rf.matchIndex[index] = 0
					}
					rf.logs = append(rf.logs, LogEntry{
						Term:   rf.currentTerm,
						Index:  HeartBeatsIndex,
						Buffer: nil,
					})
					rf.logIndex++
					rf.persist()
					done <- struct{}{}
					rf.mu.Unlock()
					go HeartBeats(rf)
					return
				} else if reject > sz/2 {
					rf.mu.Lock()
					rf.identity = Follower
					rf.voteFor = VoteForNull
					rf.leaderID = InvalidLeader
					done <- struct{}{}
					if rf.currentTerm < maxTerm {
						rf.currentTerm = maxTerm
					}
					rf.mu.Unlock()
					return
				}
				if accept+reject == sz {
					rf.mu.Lock()
					if rf.currentTerm < maxTerm {
						rf.currentTerm = maxTerm
					}
					rf.mu.Unlock()
					done <- struct{}{}
					return
				}
			}
		}(ac, sz, rf.currentTerm)
		rf.mu.Unlock()
		<-done
	}
}
func (rf *Raft )acceptCommit (index int) {
	atomic.AddInt32(&rf.heartBeatsID, 1)
	rf.mu.Lock()
	i := rf.commitIndex + 1
	for ; i <= index; i++ {
		rf.commitIndex = i
		if rf.logs[i].Index != HeartBeatsIndex {
			rf.commitLogIndex = rf.logs[i].Index
		}
		if rf.logs[i].Buffer != nil {
			msg := ApplyMsg{
				CommandValid: rf.logs[i].Buffer != nil,
				Command:      rf.logs[i].Buffer,
				CommandIndex: rf.logs[i].Index,
			}
			rf.INFO_LOG(fmt.Sprintf("leader send %d", msg.Command))
			rf.applyChan <- msg
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendLogEntries (server int, ac chan bool, logIndex int, lastIndex int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	ok := rf.sendAppendEntries(server, &args, &reply)
	ac <- reply.Success
	rf.mu.Lock()
	if rf.identity != Leader {
		rf.mu.Unlock()
		return
	}
	rf.INFO_LOG(fmt.Sprintf("Success %t logIndex is %d", reply.Success, logIndex))
	if reply.Success {
		if logIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = logIndex
			rf.nextIndex[server] = logIndex + 1
		}
	} else if ok && lastIndex == rf.nextIndex[server] {
		i := lastIndex - 1
		rf.INFO_LOG(fmt.Sprintf("before is %d", lastIndex))
		for ; i > 0; i-- {
			if rf.logs[i-1].Term != rf.logs[lastIndex - 1].Term {
				i++
				break
			}
		}
		if i == lastIndex {
			i = i - 1
		}
		if i > rf.matchIndex[server] + 1 && i <= rf.nextIndex[server] {
			rf.nextIndex[server] = i
		} else if rf.nextIndex[server] > rf.matchIndex[server] + 1 {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		}
		rf.INFO_LOG(fmt.Sprintf("after is %d", rf.nextIndex[server]))
	}
	rf.mu.Unlock()
}

func HeartBeats(rf *Raft) {
	for {
		rf.mu.Lock()
		if rf.identity != Leader {
			rf.INFO_LOG("server is no longer the leader")
			rf.mu.Unlock()
			return
		}
		ac := make(chan bool, len(rf.peers))
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			args := AppendEntriesArgs {
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PreLogIndex:  rf.logs[rf.nextIndex[index]-1].Index,
				PreLogTerm:   rf.logs[rf.nextIndex[index]-1].Term,
				Entries:      make([]LogEntry, 0, 0),
				LeaderCommit: rf.logs[rf.matchIndex[index]].Index,
			}
			if args.LeaderCommit > rf.commitLogIndex {
				args.LeaderCommit = rf.commitLogIndex
			}
			rf.INFO_LOG(fmt.Sprintf("rf.nextIndex[index] %d rf.matchIndex[index] %d server %d", rf.nextIndex[index], rf.matchIndex[index], index))
			for _, entry := range rf.logs[rf.nextIndex[index]:] {
				args.Entries = append(args.Entries, entry)
			}
			go rf.sendLogEntries(index, ac, rf.logIndex, rf.nextIndex[index], args)
		}
		nums := len(rf.peers)
		idx := rf.logIndex
		go func(nums, index int, ac chan bool) {
			finished := 1
			accept := 1
			for {
				ret := <-ac
				if ret {
					accept++
					if accept > nums/2 {
						go rf.acceptCommit(index)
						return
					}
				}
				finished++
				if finished == nums {
					return
				}
			}
		}(nums, idx, ac)
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * HeartBeatsInterval)
	}
}
func tick(rf *Raft) {
	for {
		lid := atomic.LoadInt32(&rf.heartBeatsID)
		time.Sleep(time.Millisecond * time.Duration(ElectTimeOutSleepTimeStart + rand.Intn(ElectTimeOutInterval)))
		nid := atomic.LoadInt32(&rf.heartBeatsID)
		if lid == nid {
			rf.mu.Lock()
			rf.identity = Follower
			rf.voteFor = VoteForNull
			rf.leaderID = InvalidLeader
			rf.mu.Unlock()
			ElectionMain(rf)
		}
	}
}
func RaftMain(rf *Raft) {
	go tick(rf)
	rf.mu.Lock()
	if rf.identity == Leader {
		go HeartBeats(rf)
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.status = false
	rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.leaderID = InvalidLeader
	rf.logIndex = 0
	rf.voteFor = VoteForNull
	rf.identity = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.commitLogIndex = 0
	rf.logs = make([]LogEntry, 1, 10)
	atomic.StoreInt32(&rf.heartBeatsID, 0)
	rf.status = true
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go RaftMain(rf)

	return rf
}
