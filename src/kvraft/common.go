package raftkv

import "log"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq			int
	ClientID	int64
}

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type PutAppendReply struct {
	IsLeader	bool
	Error       Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	IsLeader 		bool
	Error        	Err
	Value       	string
}

func (args *GetArgs) copy() (ret GetArgs){
	ret = GetArgs{args.Key}
	return
}

func (args *PutAppendArgs) copy() (ret PutAppendArgs) {
	ret = PutAppendArgs{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		Seq:      args.Seq,
		ClientID: args.ClientID,
	}
	return
}