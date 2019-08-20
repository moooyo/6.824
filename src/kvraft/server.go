package raftkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

func (kv *KVServer) DPrintf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S][%d]", kv.me)
	tmp := fmt.Sprintf(format, a...)
	DPrintf("%s %s", prefix, tmp)
}

// operation
const (
	PutOp    = "Put"
	AppendOp = "Append"
)

// Err
const (
	ErrWrongLeader = "Wrong Leader"
)

//
// snapshot rate.
// if state size * rate > max size
// server need create a snapshot
//
const SnapshotRate = 0.9

type NotifyArgs struct {
	Error Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shutdown  chan struct{}
	notify    map[int]chan NotifyArgs
	data      map[string]string
	ack       map[int64]int
	persister *raft.Persister
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) UnLock() {
	kv.mu.Unlock()
}

const StartTimeOut = time.Millisecond * 150

func (kv *KVServer) Start(args interface{}) (Err, string) {
	index, _, ok := kv.rf.Start(args)
	if !ok {
		return ErrWrongLeader, ""
	}
	kv.Lock()
	notify := make(chan NotifyArgs, 1)
	kv.notify[index] = notify
	kv.UnLock()
	select {
	case <-time.After(StartTimeOut):
		kv.Lock()
		if _, ok := kv.notify[index]; ok {
			delete(kv.notify, index)
		}
		kv.UnLock()
		return ErrWrongLeader, ""
	case reply := <-notify:
		return reply.Error, reply.Value
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	err, value := kv.Start(args.copy())
	reply.Error = err
	reply.IsLeader = reply.Error != ErrWrongLeader
	reply.Value = value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	err, _ := kv.Start(args.copy())
	reply.Error = err
	reply.IsLeader = reply.Error != ErrWrongLeader
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.Lock()
	kv.shutdown <- struct{}{}
	kv.UnLock()
}

//
// before you call this
// function. you need lock
// KV server Mutex
//
func (kv *KVServer) ReplyNotify(index int, reply NotifyArgs) {
	if channel, ok := kv.notify[index]; ok {
		delete(kv.notify, index)
		channel <- reply
	}
}
func (kv *KVServer) CreateSnapShot(idx int) {
	if kv.persister.RaftStateSize() >= int(SnapshotRate * float64(kv.maxraftstate)) {
		// need create snapshot
		buffer := new(bytes.Buffer)
		enc := labgob.NewEncoder(buffer)
		if enc.Encode(kv.data) != nil || enc.Encode(kv.ack) != nil {
			log.Fatal("snapshot err")
		}
		data := buffer.Bytes()
		kv.rf.CreateSnapShot(idx, data)
	}
}

func (kv *KVServer) InstallSnapShot(msg raft.ApplyMsg){
	kv.Lock()
	kv.RecoveryFromSnapShot()
	kv.UnLock()
}

func (kv *KVServer) RecoveryFromSnapShot(){
	snapshot := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(r)
	kv.data = nil
	kv.ack = nil
	var data map[string]string
	var ack map[int64]int
	if err := dec.Decode(&data); err != nil {
		data = make(map[string]string)
	}
	if err := dec.Decode(&ack); err != nil {
		ack = make(map[int64]int)
	}
	kv.data = data
	kv.ack = ack
}

func (kv *KVServer) Do(msg raft.ApplyMsg) {
	args := NotifyArgs{
		Error: "",
		Value: "",
	}
	kv.Lock()
	index := msg.CommandIndex
	if tmp, ok := msg.Command.(GetArgs); ok {
		args.Value = kv.data[tmp.Key]
		kv.DPrintf("Get %s = %s", tmp.Key, args.Value)
	} else if tmp, ok := msg.Command.(PutAppendArgs); ok {
		kv.DPrintf("%s = %s", tmp.Key, tmp.Value)
		client := tmp.ClientID
		if kv.ack[client] == tmp.Seq-1 {
			kv.ack[client]++
			if tmp.Op == PutOp {
				kv.data[tmp.Key] = tmp.Value
				kv.DPrintf("putop %s = %s tmp.value = %s", tmp.Key, kv.data[tmp.Key], tmp.Value)
			} else if tmp.Op == AppendOp {
				kv.data[tmp.Key] += tmp.Value
				kv.DPrintf("append %s = %s tmp.value = %s", tmp.Key, kv.data[tmp.Key], tmp.Value)

			} else {
				kv.DPrintf("AppendArgs.op err")
				args.Error = ErrWrongLeader
			}
		} else if kv.ack[client] < tmp.Seq-1 {
			kv.DPrintf("accept but not ack")
			args.Error = ErrWrongLeader
		} else {
			kv.DPrintf("resend ack msg ack %d seq %d client %d ", kv.ack[client], tmp.Seq, tmp.ClientID)
		}
	}
	kv.ReplyNotify(index, args)
	kv.CreateSnapShot(index)
	kv.UnLock()
}

func (kv *KVServer) Run() {
	for {
		select {
		case <-kv.shutdown:
			return
		case msg := <- kv.applyCh:
			if msg.CommandValid {
				kv.Do(msg)
			} else if msg.CommandType == raft.InstallSnapshotType {
				kv.DPrintf("install")
				kv.InstallSnapShot(msg)
			}
		}
	}
}

func StartInit() {
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	StartInit()

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.shutdown = make(chan struct{}, 1)
	kv.notify = make(map[int]chan NotifyArgs)

	kv.RecoveryFromSnapShot()

	go kv.Run()

	return kv
}
