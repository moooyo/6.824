package raftkv

import (
	"fmt"
	"labrpc"
	"raft"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

func (ck *Clerk) DPrintf (format string, a ...interface{})  {
	prefix := fmt.Sprintf("[C][%d][%d][%d]", ck.ClientID, ck.LeaderID, ck.Sequence)
	tmp := fmt.Sprintf(format, a...)
	DPrintf("%s %s",prefix, tmp)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Sequence		int 			// seq
	ClientID		int64			// Client ID
	LeaderID		int				// Leader ID

	// mu
	mutex 			sync.Mutex
}

func (ck *Clerk) Lock ()  {
	ck.mutex.Lock()
}

func (ck *Clerk) UnLock () {
	ck.mutex.Unlock()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.Sequence = 0
	ck.LeaderID = raft.InvalidLeader
	ck.servers = servers

	return ck
}

const RetryInterval = time.Duration(125 * time.Millisecond)

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.Lock()
	server := ck.LeaderID
	ck.UnLock()
	if server == raft.InvalidLeader {
		server = 0
	}
	args := GetArgs{Key:key}
	for {
		reply := GetReply{
			IsLeader: false,
			Error:       "",
			Value:       "",
		}
		_ = ck.servers[server].Call("KVServer.Get", &args, &reply)
		if reply.IsLeader {
			ck.Lock()
			ck.LeaderID = server
			ck.UnLock()
			return reply.Value
		}

		server = (server + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	ck.Lock()
	server := ck.LeaderID
	args.ClientID = ck.ClientID
	ck.Sequence++
	args.Seq = ck.Sequence
	ck.UnLock()

	if server == raft.InvalidLeader {
		server = 0
	}

	for {
		reply := PutAppendReply{
			IsLeader: false,
			Error:    "",
		}
		_ = ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if  reply.IsLeader {
			ck.Lock()
			ck.LeaderID = server
			ck.UnLock()
			ck.DPrintf("append <%s,%s> success", args.Key, args.Value)
			return
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
