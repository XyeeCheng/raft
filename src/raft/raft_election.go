package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// resetElectionTimerLocked 方法用于重置选举计时器。
// 此方法需要在持有锁的情况下调用，以确保线程安全。
// 它会记录当前时间作为选举开始时间，并随机生成一个选举超时时间。
func (rf *Raft) resetElectionTimerLocked() {
	// 记录当前时间作为选举开始时间
	rf.electionStart = time.Now()
	// 计算选举超时时间的随机范围
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	// 随机生成一个选举超时时间，范围在 electionTimeoutMin 到 electionTimeoutMax 之间
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// isElectionTimeoutLocked 方法用于检查选举是否超时。
// 此方法需要在持有锁的情况下调用，以确保线程安全。
// 它会比较当前时间与选举开始时间和选举超时时间的差值。
func (rf *Raft) isElectionTimeoutLocked() bool {
	// 计算从选举开始到现在的时间间隔
	// 如果该时间间隔大于选举超时时间，则返回 true，表示选举超时
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// isMoreUpToDateLocked 方法用于检查当前节点的最后一个日志条目是否比候选者的最后一个日志条目更新。
// 此方法需要在持有锁的情况下调用，以确保线程安全。
// 它会比较两个日志条目的任期号和索引。
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	// 获取当前节点最后一个日志条目的索引和任期号
	lastIndex, lastTerm := rf.log.last()

	// 记录比较信息，用于调试
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	// 如果任期号不同，任期号大的日志条目更新
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	// 如果任期号相同，索引大的日志条目更新
	return lastIndex > candidateIndex
}

// RequestVoteArgs 结构体定义了 RequestVote RPC 的参数。
// 当一个节点成为候选者时，会使用该结构体向其他节点发送投票请求。
type RequestVoteArgs struct {
	// 候选者的当前任期号
	Term int
	// 候选者的节点 ID
	CandidateId int
	// 候选者最后一个日志条目的索引
	LastLogIndex int
	// 候选者最后一个日志条目的任期号
	LastLogTerm int
}

// String 方法为 RequestVoteArgs 结构体提供了一个字符串表示形式，方便调试和日志记录。
func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last: [%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

// RequestVoteReply 结构体定义了 RequestVote RPC 的回复。
// 当一个节点收到投票请求时，会使用该结构体向候选者回复投票结果。
type RequestVoteReply struct {
	// 接收者的当前任期号
	Term int
	// 是否授予投票
	VoteGranted bool
}

// String 方法为 RequestVoteReply 结构体提供了一个字符串表示形式，方便调试和日志记录。
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

// RequestVote 方法是 RequestVote RPC 的处理函数。
// 当一个节点收到其他节点的投票请求时，会调用此方法处理请求。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 加锁以保护共享状态，防止并发问题
	rf.mu.Lock()
	// 确保在函数返回时解锁
	defer rf.mu.Unlock()
	// 记录收到的投票请求的日志，用于调试
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	// 初始化回复的任期号为当前节点的任期号
	reply.Term = rf.currentTerm
	// 初始化回复的投票授予标志为 false
	reply.VoteGranted = false
	// 处理任期号不一致的情况
	// 如果候选者的任期号小于当前节点的任期号，拒绝投票请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	// 如果候选者的任期号大于当前节点的任期号，将当前节点转变为跟随者，并更新任期号
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 检查是否已经投票给其他节点
	// 如果已经投票给其他节点，拒绝投票请求
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.votedFor)
		return
	}

	// 检查候选者的最后一个日志条目是否比当前节点的最后一个日志条目更新
	// 如果当前节点的最后一个日志条目更新，拒绝投票请求
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return
	}

	// 如果上述检查都通过，授予投票
	reply.VoteGranted = true
	// 记录投票给的候选者 ID
	rf.votedFor = args.CandidateId
	// 持久化当前节点的状态
	rf.persistLocked()
	// 重置选举计时器
	rf.resetElectionTimerLocked()
	// 记录授予投票的日志，用于调试
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
}

// sendRequestVote 方法用于向指定的服务器发送 RequestVote RPC 请求。
// server 是目标服务器在 rf.peers 数组中的索引。
// args 是 RequestVote RPC 的参数。
// reply 是用于接收 RPC 回复的结构体指针。
// 该方法会调用 labrpc 库的 Call 方法发送请求，并返回是否成功收到回复。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// 调用 labrpc 库的 Call 方法发送 RPC 请求
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// startElection 方法用于启动选举过程。
// 当一个节点成为候选者时，会调用此方法向其他节点发送投票请求，并处理投票回复。
// term 是当前的任期号，确保在该任期内进行选举操作。
func (rf *Raft) startElection(term int) {
	// 初始化收到的投票数为 0
	votes := 0
	// 定义一个匿名函数，用于向指定节点发送投票请求并处理回复
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		// 创建一个用于接收回复的结构体
		reply := &RequestVoteReply{}
		// 向指定节点发送投票请求
		ok := rf.sendRequestVote(peer, args, reply)

		// 加锁以保护共享状态，防止并发问题
		rf.mu.Lock()
		// 确保在函数返回时解锁
		defer rf.mu.Unlock()
		// 如果请求失败，记录日志
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		// 记录收到的投票回复的日志，用于调试
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		// 处理任期号不一致的情况
		// 如果回复中的任期号大于当前节点的任期号，将当前节点转变为跟随者
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 检查上下文是否丢失
		// 如果当前节点不再是候选者或任期号发生变化，放弃处理该回复
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
			return
		}

		// 统计投票数
		// 如果收到的回复中授予了投票，增加投票数
		if reply.VoteGranted {
			votes++
			// 如果收到的投票数超过节点总数的一半，成为领导者
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				// 启动日志复制过程
				go rf.replicationTicker(term)
			}
		}
	}

	// 加锁以保护共享状态，防止并发问题
	rf.mu.Lock()
	// 确保在函数返回时解锁
	defer rf.mu.Unlock()
	// 检查上下文是否丢失
	// 如果当前节点不再是候选者或任期号发生变化，放弃启动选举过程
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d], abort RequestVote", rf.role, term, rf.currentTerm)
		return
	}

	// 获取当前节点最后一个日志条目的索引和任期号
	lastIdx, lastTerm := rf.log.last()
	// 遍历所有节点，发送投票请求
	for peer := 0; peer < len(rf.peers); peer++ {
		// 如果是当前节点自身，给自己投一票
		if peer == rf.me {
			votes++
			continue
		}

		// 创建投票请求的参数
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		// 记录发送投票请求的日志，用于调试
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())
		// 启动一个 goroutine 发送投票请求
		go askVoteFromPeer(peer, args)
	}
}

// electionTicker 方法是一个循环函数，用于定期检查是否需要启动选举。
// 只要节点没有被停止，该函数就会持续运行。
func (rf *Raft) electionTicker() {
	// 只要节点未停止，就持续运行
	for !rf.killed() {
		// 加锁以保护共享状态，防止并发问题
		rf.mu.Lock()
		// 如果当前节点不是领导者，并且选举超时，启动选举
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			// 将当前节点转变为候选者
			rf.becomeCandidateLocked()
			// 启动选举过程
			go rf.startElection(rf.currentTerm)
		}
		// 解锁
		rf.mu.Unlock()

		// 暂停一段时间，时间范围在 50 到 350 毫秒之间
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
