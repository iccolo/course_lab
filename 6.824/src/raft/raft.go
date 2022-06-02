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
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh               chan ApplyMsg
	persistenceObj        *persistenceObj // 需要持久化的数据，在Raft创建时读取并初始化
	leaderHeartBeat       chan struct{}   // 接收Leader心跳并更新定时器，在Raft创建时初始化
	peersMaxLogIndexes    []int           // 作为Leader时，其他节点的Max Log Index，初始化时会设定为Leader的Max Log Index
	peersAckMaxLogIndexes []int           // 作为Leader时，其他节点的Max Ack Log Index，初始化设定-1
	tryApply              chan struct{}   // 通知Apply goroutine发送ApplyMsg
	maxAppliedLogIndex    int             // 作为Follower时，Max Applied Log Index，初始化设定-1
}

func (rf *Raft) Debug(format string, args ...interface{}) {
	prefix := fmt.Sprintf("[node %d]", rf.me)
	log.Printf(prefix+format, args...)
}

func (rf *Raft) Error(format string, args ...interface{}) {
	prefix := fmt.Sprintf("[node %d]", rf.me)
	log.Printf(prefix+format, args...)
}

type persistenceObj struct {
	curTerm int
	voteFor int
	slots   []*Log
}

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

func (rf *Raft) getLastLogInfo() (index, term int) {
	slots := rf.persistenceObj.slots
	if len(slots) == 0 {
		return 0, 0
	}
	lastLog := slots[len(slots)-1]
	return lastLog.Index, lastLog.Term
}

func (rf *Raft) getLastLogIndex() int {
	slots := rf.persistenceObj.slots
	if len(slots) == 0 {
		return 0
	}
	return slots[len(slots)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	slots := rf.persistenceObj.slots
	if len(slots) == 0 {
		return 0
	}
	return slots[len(slots)-1].Term
}

func (rf *Raft) getLatestNLog(n int) *Log {
	slots := rf.persistenceObj.slots
	if n <= 0 || len(slots) <= n {
		return nil
	}
	return slots[len(slots)-n]
}

func (rf *Raft) getLogByIndex(index int) *Log {
	slots := rf.persistenceObj.slots
	if len(slots) == 0 {
		return nil
	}
	i := index - slots[0].Index
	if i < 0 || i >= len(slots){
		return nil
	}
	return slots[i]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.persistenceObj.curTerm, rf.me == rf.persistenceObj.voteFor && len(rf.peers)/len(rf.votes) < 2
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
	enc := labgob.NewEncoder(w)
	if err := enc.Encode(rf.persistenceObj); err != nil {
		DPrintf("persis encode err:%v", err)
	}
	rf.persister.SaveRaftState(w.Bytes())
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
	rf.persistenceObj = new(persistenceObj)
	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(rf.persistenceObj); err != nil {
		panic(err)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From int
	Term int
	LatestLogIndex int
	LatestLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	IsAgree bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Debug("receive RequestVote, args:%+v", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LatestLogTerm > rf.persistenceObj.curTerm ||
		(args.LatestLogTerm == rf.persistenceObj.curTerm && args.LatestLogIndex >= rf.getLastLogIndex()) {
		rf.persistenceObj.curTerm = args.Term
		rf.persistenceObj.voteFor = args.From
		rf.persist()
		reply.IsAgree = true
		rf.Debug("accept node %d as leader", args.From)
		return
	}
	rf.Debug("refuse node %d as leader", args.From)
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
	rf.Debug("RequestVote args:%+v", args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.Debug("RequestVote reply:%+v", reply)
	return ok
}

type AppendEntryArgs struct {
	From int // 源节点
	Term int // Leader任期号
	PrevLogIndex int // Follower在相应位置有log且log term与PrevLogTerm一致才能用此消息更新log
	PrevLogTerm  int
	Slots        []*Log // PrevLogIndex之后的的所有Log，不包含PrevLogIndex位置
}

// AppendEntryReply 支持follower快速恢复log的响应
type AppendEntryReply struct {
	XTerm  int // 这个是Follower中与Leader冲突的Log对应的任期号。在之前（7.1）有介绍Leader会在prevLogTerm中带上本地Log记录中，前一条Log的任期号。如果Follower在对应位置的任期号不匹配，它会拒绝Leader的AppendEntry消息，并将自己的任期号放在XTerm中。如果Follower在对应位置没有Log，那么这里会返回 -1。
	XIndex int // 这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号。
	XLen   int // 如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log槽位数。
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.Debug("receive AppendEntry, args:%+v",  args)
	if args.Term < rf.persistenceObj.curTerm || (args.Term == rf.persistenceObj.curTerm && args.From != rf.persistenceObj.voteFor){
		rf.Error("invalid AppendEntry:%+v", args) // TODO how to fill rsp
		return
	}

	// 接收首次Log
	if args.PrevLogIndex == 0 {
		rf.persistenceObj.slots = args.Slots
		rf.persistenceObj.curTerm = args.Term
		rf.persistenceObj.voteFor = args.From
		return
	}

	// Follower对应位置没有Log
	preLog := rf.getLogByIndex(args.PrevLogIndex)
	if preLog == nil {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.getLastLogIndex()
		return
	}
	// 有Log
	reply.XTerm = preLog.Term
	reply.XIndex = args.PrevLogIndex
	for ; reply.XIndex - 1 > 0;  {
		if l := rf.getLogByIndex(reply.XIndex -1); l == nil || l.Term != reply.XTerm {
			break
		}
		reply.XIndex--
	}
	// 对应位置Log Term相同，更新Log
	if preLog.Term == args.PrevLogTerm {
		rf.persistenceObj.curTerm = args.Term
		rf.persistenceObj.voteFor = args.From
		rf.persistenceObj.slots = append(rf.persistenceObj.slots[:], args.Slots...)

	}
	lastLog := rf.getLatestNLog(1)
	if lastLog == nil  {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex+1
		return
	}
	if lastLog.Index < args.PrevLogIndex {

		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.getLatestNLog(1).Index
		return
	}

	selfPrevLog := rf.getLogByIndex(args.PrevLogIndex)

	if args.PrevLogIndex <= rf.getLatestNLog(1).Index {
		if ;selfLog != nil && args.PrevLogTerm == selfLog.LeaderTerm {
			for i, log := range args.Slots {

			}
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.Debug("begin AppendEntry to node %d, args:%+v", server, args)
	return rf.peers[server].Call("Raft.AppendEntry", args, reply)
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// 串行化追加log
	rf.mu.Lock()
	term, isLeader = rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	index = rf.getLatestNLog(1).Index + 1
	rf.persistenceObj.slots = append(rf.persistenceObj.slots, &Log{
		Index:   index,
		Term:    term,
		Command: command,
	})
	labgob.Register(command)
	rf.persist()
	rf.mu.Unlock()
	// 通知其他节点
	go rf.syncAllPeer()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if _, isLeader := rf.GetState(); isLeader {
			rf.Debug("is leader, no ticker")
			return
		}
		interval := getElectionTimerInterval()
		t := time.NewTimer(interval)
		rf.Debug("start election timer, timer interval:%v", rf.me, interval)
		select {
		case <-rf.leaderHeartBeat:
			interval = getElectionTimerInterval()
			t = time.NewTimer(interval)
			rf.Debug("restart election timer, timer interval:%v", rf.me, interval)
		case <-t.C:
			rf.campaign()
		}
	}
	rf.Debug("be killed")
}

const (
	leaderHeartBeatInterval      = 3000 // 心跳间隔，单位ms
	electionTimerIntervalRateMin = 2    // 最小选举定时器与心跳间隔比值
	electionTimerIntervalRateMax = 3    // 最大选举定时器与心跳间隔比值
)

func getElectionTimerInterval() time.Duration {
	minInterval := electionTimerIntervalRateMin * leaderHeartBeatInterval
	maxInterval := electionTimerIntervalRateMax * leaderHeartBeatInterval
	r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	randAdd := r.Intn(maxInterval - minInterval)
	return time.Duration(minInterval+randAdd) * time.Millisecond
}

// 竞选Leader：任期号增加 && 投票给自己 && 发送 RequestVote
func (rf *Raft) campaign() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newTerm := rf.persistenceObj.curTerm + 1
	votesCnt := int64(1) // 当前任期获得的Leader选票数量

	rf.Debug("begin campaign term:%d", newTerm)

	wg := new(sync.WaitGroup)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := &RequestVoteArgs{
				From: rf.me,
				Term: newTerm,
			}
			req.LatestLogIndex, req.LatestLogTerm = rf.getLastLogInfo()
			rsp := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, req, rsp); ok && rsp.IsAgree {
				rf.Debug("won votes from node %d in term %d", newTerm, i)
				atomic.AddInt64(&votesCnt, 1)
			}
		}(i)
	}
	wg.Wait()
	rf.Debug("votesCnt %d", votesCnt)
	// 判断投票结果
	if votesCnt*2 <= int64(len(rf.peers)) {
		return
	}
	rf.persistenceObj.curTerm++
	rf.persistenceObj.voteFor = rf.me
	rf.persist()
	rf.Debug("being leader, term %v", rf.persistenceObj.curTerm)
	go rf.afterWonCampaign()
}

// 竞选成功：建立PeerMaxLogIndex && 发送 AppendEntry
func (rf *Raft) afterWonCampaign() {
	rf.initPeerMaxLogIndex()
	rf.initPeersAckMaxLogIndexes()
	go rf.leaderApply()
	rf.syncAllPeer()
}

// Leader初始化PeerMaxLogIndex，刚开始所有节点的Max Log Index设置为与Leader一致
func (rf *Raft) initPeerMaxLogIndex() {
	rf.peersMaxLogIndexes = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.peersMaxLogIndexes[i] = rf.getLastLogIndex()
	}
}

// Leader初始化PeerAckMaxLogIndex，刚开始所有节点的Max Ack Log Index设置为0
func (rf *Raft) initPeersAckMaxLogIndexes() {
	rf.peersAckMaxLogIndexes = make([]int, len(rf.peers))
}

func (rf *Raft) syncAllPeer() {
	// 通知其他节点对齐Log
	wg := new(sync.WaitGroup)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			rf.syncPeer(server)
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) syncPeer(server int) {
	req := rf.genAppendEntry(server)
	rsp := &AppendEntryReply{}
	if ok := rf.sendAppendEntry(server, req, rsp); !ok {
		rf.Debug("sendAppendEntry to node %d fail", server)
		return
	}
	// Follower在相同位置有相同Log，说明Log已对齐
	if rsp.XTerm == req.PrevLogTerm {
		ackIndex := calculateAckIndex(req)
		rf.peersMaxLogIndexes[server] = ackIndex
		rf.peersAckMaxLogIndexes[server] = ackIndex
		rf.tryApply <- struct{}{}
		rf.Debug("node %d ack index %d", server, ackIndex)
		return
	}
	// Follower在pre log index没有log，需要再次同步
	if rsp.XTerm == -1 {
		rf.Debug("node %d has no log at preIndex %d, will sync again", server, req.PrevLogIndex)
		rf.peersMaxLogIndexes[server] = req.PrevLogIndex - rsp.XLen
		rf.syncPeer(server)
		return
	}
	rf.Debug("node %d has diff log term %d after index %d, will sync again", server, rsp.XTerm, rsp.XIndex)
	// Follower在pre log index有不同的log，需要将整个任期的log同步过去进行覆盖
	rf.peersMaxLogIndexes[server] = rsp.XIndex - 1
	rf.syncPeer(server)
}

func (rf *Raft) genAppendEntry(server int) *AppendEntryArgs {
	var (
		preIndex = rf.peersMaxLogIndexes[server]
		preTerm = 0
	)
	if l := rf.getLogByIndex(preIndex); l != nil {
		preTerm = l.Term
	}
	// TODO 一次发送的Slots需要有个上限
	return &AppendEntryArgs{
		Term : rf.persistenceObj.curTerm,
		PrevLogIndex: preIndex,
		PrevLogTerm:  preTerm,
		Slots:        rf.GetLogGtIndex(preIndex),
	}
}

// GetLogGtIndex 获取index以后的log，顺序排列
func (rf *Raft) GetLogGtIndex(index int) []*Log {
	slots := rf.persistenceObj.slots
	if len(slots) == 0 {
		return nil
	}
	if index+1 < slots[0].Index {
		rf.Error("input index %d invalid, min log index:%d", index, slots[0].Index)
		return nil
	}
	i := index+1 - slots[0].Index
	var logs []*Log
	for ; i < len(slots); i++ {
		logs = append(logs, slots[i])
	}
	return logs
}

func calculateAckIndex(req *AppendEntryArgs) int {
	if len(req.Slots) == 0 {
		return req.PrevLogIndex
	}
	return req.Slots[len(req.Slots)-1].Index
}

// leaderApply
func (rf *Raft) leaderApply() {
	rf.tryApply = make(chan struct{}, 10)
	for {
		<-rf.tryApply
		follows := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.peersAckMaxLogIndexes[i] > rf.maxAppliedLogIndex {
				follows++
			}
		}
		if follows*2 < len(rf.peers) {
			break
		}
		rf.maxAppliedLogIndex++
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.getLogByIndex(rf.maxAppliedLogIndex).Command,
			CommandIndex:  rf.maxAppliedLogIndex,
			SnapshotValid: false,
		}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.leaderHeartBeat = make(chan struct{}, 10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	rf.Debug("started")
	return rf
}
