package raft

//
// 这是 Raft 必须向服务（或测试程序）公开的 API 大纲。
// 有关每个函数的更多详细信息，请参阅下面的注释。
//
// rf = Make(...)
//   创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   开始对新的日志条目达成共识。
// rf.GetState() (term, isLeader)
//   查询 Raft 当前的任期，以及它是否认为自己是领导者。
// ApplyMsg
//   每次有新的条目提交到日志中时，每个 Raft 对等体
//   都应该通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试程序）发送一个 ApplyMsg。
//

import (
	// "bytes"

	"sync"
	"sync/atomic"
	"time"

	// "course/labgob"
	"course/labrpc"
)

// 定义选举超时时间的最小值，单位为毫秒
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	// 定义选举超时时间的最大值，单位为毫秒
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	// 定义复制间隔时间，单位为毫秒
	replicateInterval time.Duration = 30 * time.Millisecond
)

// 定义无效的任期值
const (
	InvalidTerm int = 0
	// 定义无效的索引值
	InvalidIndex int = 0
)

// 定义 Raft 节点的角色类型
type Role string

// 定义三种角色：跟随者、候选者和领导者
const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// 当每个 Raft 对等体意识到连续的日志条目已提交时，
// 该对等体应通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试程序）发送一个 ApplyMsg。
// 将 CommandValid 设置为 true 表示 ApplyMsg 包含新提交的日志条目。
//
// 在 PartD 中，你可能希望在 applyCh 上发送其他类型的消息（例如，快照），
// 但对于这些其他用途，请将 CommandValid 设置为 false。
type ApplyMsg struct {
	// 表示命令是否有效
	CommandValid bool
	// 具体的命令
	Command interface{}
	// 命令的索引
	CommandIndex int

	// 用于 PartD：
	// 表示快照是否有效
	SnapshotValid bool
	// 快照数据
	Snapshot []byte
	// 快照的任期
	SnapshotTerm int
	// 快照的索引
	SnapshotIndex int
}

// 实现单个 Raft 对等体的 Go 对象
type Raft struct {
	mu        sync.Mutex          // 锁，用于保护对该对等体状态的共享访问
	peers     []*labrpc.ClientEnd // 所有对等体的 RPC 端点
	persister *Persister          // 用于保存该对等体持久化状态的对象
	me        int                 // 该对等体在 peers[] 中的索引
	dead      int32               // 由 Kill() 设置

	// 你的数据在这里（PartA、PartB、PartC）。
	// 请参阅论文的图 2 以了解 Raft 服务器必须维护的状态。
	role        Role // 当前角色
	currentTerm int  // 当前任期
	votedFor    int  // -1 表示未投票给任何人
	// 本地日志
	log *RaftLog

	// 仅在领导者中使用
	// 每个对等体的视图
	nextIndex  []int // 下一个要发送给每个对等体的日志索引
	matchIndex []int // 每个对等体已匹配的日志索引

	// 应用循环的字段
	commitIndex int           // 已提交的索引
	lastApplied int           // 最后应用的索引
	applyCh     chan ApplyMsg // 用于发送 ApplyMsg 的通道
	snapPending bool          // 是否有待处理的快照
	applyCond   *sync.Cond    // 条件变量，用于应用循环的同步

	electionStart   time.Time     // 选举开始时间
	electionTimeout time.Duration // 选举超时时间（随机）
}

// 将节点角色变为跟随者（需持有锁）
func (rf *Raft) becomeFollowerLocked(term int) {
	// 如果传入的任期小于当前任期，不能变为跟随者，记录错误日志并返回
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}

	// 记录角色变化日志
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	// 判断是否需要持久化（任期改变时需要）
	shouldPersit := rf.currentTerm != term
	// 如果新任期大于当前任期，重置投票信息
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	// 如果需要持久化，调用持久化方法
	if shouldPersit {
		rf.persistLocked()
	}
}

// 将节点角色变为候选者（需持有锁）
func (rf *Raft) becomeCandidateLocked() {
	// 如果当前角色是领导者，不能变为候选者，记录错误日志并返回
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	// 记录角色变化日志
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persistLocked()
}

// 将节点角色变为领导者（需持有锁）
func (rf *Raft) becomeLeaderLocked() {
	// 如果当前角色不是候选者，不能变为领导者，记录错误日志并返回
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	// 记录角色变化日志
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
	// 初始化每个对等体的 nextIndex 和 matchIndex
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}

// 返回当前任期和该服务器是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {
	// 加锁以保护共享状态
	rf.mu.Lock()
	// 函数返回时解锁
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// 获取 Raft 状态的大小
func (rf *Raft) GetRaftStateSize() int {
	// 加锁以保护共享状态
	rf.mu.Lock()
	// 函数返回时解锁
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 使用 Raft 的服务（例如 k/v 服务器）希望开始对要追加到 Raft 日志的下一个命令达成共识。
// 如果此服务器不是领导者，则返回 false。否则，开始共识并立即返回。
// 无法保证此命令最终会提交到 Raft 日志中，因为领导者可能会失败或失去选举。
// 即使 Raft 实例已被终止，此函数也应正常返回。
//
// 第一个返回值是如果命令最终提交，它将出现在的索引。
// 第二个返回值是当前任期。
// 第三个返回值是如果此服务器认为自己是领导者则为 true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 加锁以保护共享状态
	rf.mu.Lock()
	// 函数返回时解锁
	defer rf.mu.Unlock()

	// 如果当前角色不是领导者，返回无效值
	if rf.role != Leader {
		return 0, 0, false
	}
	// 向日志中追加新的日志条目
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	// 记录领导者接受日志的日志
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()

	return rf.log.size() - 1, rf.currentTerm, true
}

// 测试程序在每次测试后不会停止 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。你的代码可以使用 killed() 来
// 检查 Kill() 是否已被调用。使用原子操作避免了使用锁的需要。
//
// 问题在于长时间运行的 goroutine 会占用内存，并且可能会消耗 CPU 时间，
// 这可能会导致后续测试失败并产生混乱的调试输出。
// 任何具有长时间运行循环的 goroutine 都应调用 killed() 来检查是否应停止。
func (rf *Raft) Kill() {
	// 设置节点已终止的标志
	atomic.StoreInt32(&rf.dead, 1)
	// 你可以根据需要在此处添加其他代码。
}

// 检查节点是否已被终止
func (rf *Raft) killed() bool {
	// 加载节点已终止的标志
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 检查上下文是否丢失（需持有锁）
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	// 如果当前任期和角色与传入的不匹配，则认为上下文丢失
	return !(rf.currentTerm == term && rf.role == role)
}

// 服务或测试程序希望创建一个 Raft 服务器。
// 所有 Raft 服务器（包括此服务器）的端口都在 peers[] 中。
// 此服务器的端口是 peers[me]。所有服务器的 peers[] 数组
// 具有相同的顺序。persister 是此服务器用于保存其持久化状态的地方，
// 并且如果有任何最近保存的状态，它最初也会持有该状态。
// applyCh 是一个通道，测试程序或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 你的初始化代码在这里（PartA、PartB、PartC）。
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// 添加一个虚拟条目以避免大量的边界检查
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	// 初始化领导者的视图切片
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 初始化用于应用的字段
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapPending = false

	// 从崩溃前保存的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// 启动选举计时器 goroutine
	go rf.electionTicker()
	// 启动应用循环 goroutine
	go rf.applicationTicker()

	return rf
}
