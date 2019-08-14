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
	"math/rand"
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

func __LOG(str string) {
	//fmt.Println("[INFO]" + str)
}

func (rf *Raft) INFO_LOG(str string) {
	if !rf.status  {
		return
	}
	tmp := fmt.Sprintf("[%d][%d][%d][%d][%d][%d]<%p> %s ",
		rf.me, rf.currentTerm, rf.leaderID, rf.identity, rf.logIndex, rf.commitIndex, rf, str)
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
	VoteForNull   = -1
	InvalidLeader = -1
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
	currentTerm  int
	voteFor      int
	logs         []LogEntry
	logIndex     int
	commitIndex  int
	lastApplied  int
	leaderID     int
	status		 bool
	identity     int
	heartBeatsID int
	applyChan    chan ApplyMsg

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

	rf.INFO_LOG(fmt.Sprintf("recive vote request from %d", args.CandidateID))

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm &&
		rf.voteFor != VoteForNull && rf.voteFor != args.CandidateID) {
		// vote false
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.INFO_LOG(fmt.Sprintf("vote to %d false, because args.Term is %d", args.CandidateID, rf.currentTerm))
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
	if args.Term > rf.currentTerm {
		rf.INFO_LOG(fmt.Sprintf("vote for %d args.term %d", args.CandidateID, args.Term))
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateID
		//rf.identity = Follower
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.INFO_LOG(fmt.Sprintf("vote to %d false, because args.Term is %d !", args.CandidateID, rf.currentTerm))
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
func sendAppendEntriesWithoutRf(peer *labrpc.ClientEnd, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := peer.Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	//
	// correct restrict
	//
	find := false
	for i := rf.logIndex; i >= 0; i-- {
		if args.PreLogTerm == rf.logs[i].Term && args.PreLogIndex == rf.logs[i].Index {
			find = true
			break
		}
	}
	if !find {
		rf.INFO_LOG("don't find")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	//
	// restrict:
	//

	lastTerm := rf.logs[rf.commitIndex].Term
	lastIndex := rf.logs[rf.commitIndex].Index
	if (lastTerm > args.PreLogTerm) || (lastTerm == args.Term && lastIndex > args.PreLogIndex) {
		rf.INFO_LOG(fmt.Sprintf("out-of-date heartbeats from %d args.preTerm %d args.preIndex %d lastTerm %d lastIndex %d",
			args.LeaderID, args.PreLogTerm, args.PreLogIndex, lastTerm, lastIndex))
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.leaderID = args.LeaderID
	rf.currentTerm = args.Term
	rf.identity = Follower
	rf.heartBeatsID++
	rf.INFO_LOG(fmt.Sprintf("heartbeats from %d", args.LeaderID))

	reply.Term = rf.currentTerm

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
			rf.logs = rf.logs[:rf.logIndex+1]
			reply.Success = false
			rf.INFO_LOG(fmt.Sprintf("bad log now log index is %d cmd is %d",
				rf.logs[rf.logIndex].Index, rf.logs[rf.logIndex].Buffer))
		} else {
			//
			// have something more need to append
			//
			reply.Success = false
			rf.INFO_LOG(fmt.Sprintf("append false lastTerm %d lastIndex %d argsPreTerm %d argsPreIndex %d",
				lastTerm, lastIndex, args.PreLogTerm, args.PreLogIndex))
			rf.mu.Unlock()
			return
		}
	}
	for _, entries := range args.Entries {
		rf.INFO_LOG("append")
		rf.logIndex++
		rf.logs = append(rf.logs, entries)
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		for i := 0; i < len(rf.logs); i++ {
			if rf.logs[i].Index > rf.commitIndex && rf.logs[i].Index <= args.LeaderCommit {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Buffer,
					CommandIndex: rf.logs[i].Index,
				}
				rf.INFO_LOG(fmt.Sprintf("follower send %d i is %d", msg.Command.(int), i))
				rf.applyChan <- msg
				rf.commitIndex = rf.logs[i].Index
			}
		}
	}

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
			Index:  rf.logs[rf.logIndex].Index + 1,
			Buffer: command,
		}
		rf.logs = append(rf.logs, log)
		rf.logIndex++
		if isLeader {
			rf.INFO_LOG(fmt.Sprintf("Start %d", command))
		}
		index = rf.logs[rf.logIndex].Index
		term = rf.logs[rf.logIndex].Term
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

const (
	HeartBeatsInterval         = 100
	ElectTimeOutInterval       = 150
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
				}
			}(index, rf.currentTerm, ac)
		}
		sz := len(rf.peers)
		go func(ac chan bool, sz int) {
			accept := 1
			reject := 0
			for {
				ok := <-ac
				if ok {
					accept++
				} else {
					reject++
				}
				if accept > sz/2 {
					rf.mu.Lock()
					rf.INFO_LOG("elect win")
					rf.leaderID = rf.me
					rf.identity = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for index, _ := range rf.peers {
						rf.nextIndex[index] = rf.logIndex + 1
						rf.matchIndex[index] = 0
					}
					done <- struct{}{}
					rf.mu.Unlock()
					return
				} else if reject > sz/2 {
					rf.mu.Lock()
					rf.INFO_LOG("elect false")
					rf.identity = Follower
					rf.voteFor = VoteForNull
					rf.leaderID = InvalidLeader
					done <- struct{}{}
					rf.mu.Unlock()
					return
				}
				if accept + reject == sz-1 {
					done <- struct{}{}
					return
				}
			}
		}(ac, sz)
		rf.mu.Unlock()
		<- done
		rf.INFO_LOG("break elcet continue to new term")
	}
}

func LeaderMain(rf *Raft) {
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
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PreLogIndex:  rf.logs[rf.nextIndex[index]-1].Index,
				PreLogTerm:   rf.logs[rf.nextIndex[index]-1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			go func(server int, ac chan bool, logIndex int, args AppendEntriesArgs) {
				args.Entries = rf.logs[rf.nextIndex[server]:]
				reply := AppendEntriesReply{
					Term:    0,
					Success: false,
				}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok && reply.Success {
					ac <- true
					rf.mu.Lock()
					rf.matchIndex[server] = logIndex
					rf.nextIndex[server] = logIndex + 1
				} else {
					ac <- false
					rf.mu.Lock()
					if ok {
						if rf.nextIndex[server] > rf.matchIndex[server]+1 {
							rf.nextIndex[server]--
						}
					}
				}
				rf.mu.Unlock()
			}(index, ac, rf.logIndex, args)
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
						rf.mu.Lock()
						if rf.identity == Leader {
							if index >= 0 {
								i := rf.commitIndex + 1
								for ; i <= index; i++ {
									msg := ApplyMsg{
										CommandValid: true,
										Command:      rf.logs[i].Buffer,
										CommandIndex: rf.logs[i].Index,
									}
									rf.INFO_LOG(fmt.Sprintf("leader send %d", msg.Command))
									rf.applyChan <- msg
								}
							}
							rf.commitIndex = index
							rf.mu.Unlock()
							return
						}
					}
				}
				finished++
				if finished == nums-1 {
					return
				}
			}
		}(nums, idx, ac)

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * HeartBeatsInterval)
	}
}

func RaftMain(rf *Raft) {
	for {
		rf.mu.Lock()
		rf.INFO_LOG("RaftMain")
		if rf.identity == Follower {
			lastID := rf.heartBeatsID
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(ElectTimeOutSleepTimeStart+rand.Intn(ElectTimeOutInterval)))
			rf.mu.Lock()
			newID := rf.heartBeatsID
			if lastID == newID {
				rf.leaderID = InvalidLeader
				// start elect
				rf.mu.Unlock()
				ElectionMain(rf)
				continue
			}
			rf.mu.Unlock()
		} else if rf.identity == Leader {
			rf.mu.Unlock()
			LeaderMain(rf)
		}  else {
			rf.INFO_LOG("error here RaftMain")
			time.Sleep(time.Second)
			rf.mu.Unlock()
		}
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
	rf.logs = make([]LogEntry, 1, 10)
	rf.heartBeatsID = 0
	rf.status = true
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go RaftMain(rf)

	return rf
}
