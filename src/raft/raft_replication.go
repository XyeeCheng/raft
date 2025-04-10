package raft

import (
	"fmt"
	"sort"
	"time"
)

// LogEntry 代表 Raft 日志中的一个条目，包含了该条目的任期信息、命令是否有效以及具体的命令内容。
// 每个日志条目都会被持久化存储，以保证在节点故障恢复后能够继续处理。
type LogEntry struct {
	// Term 表示该日志条目创建时所在的任期号
	Term int
	// CommandValid 指示该命令是否应该被应用到状态机
	CommandValid bool
	// Command 是具体要应用到状态机的命令
	Command interface{}
}

// AppendEntriesArgs 是 AppendEntries RPC 的参数结构体，领导者使用该结构体向跟随者发送日志条目及相关信息。
// 用于同步日志，确保所有节点的日志一致。
type AppendEntriesArgs struct {
	// Term 是领导者的当前任期号，用于跟随者更新自己的任期
	Term int
	// LeaderId 标识发送该 RPC 请求的领导者节点
	LeaderId int
	// PrevLogIndex 是前一个日志条目的索引，用于检查日志的连续性
	PrevLogIndex int
	// PrevLogTerm 是前一个日志条目的任期号，用于检查日志的连续性
	PrevLogTerm int
	// Entries 是要追加到跟随者日志中的日志条目列表
	Entries []LogEntry
	// LeaderCommit 是领导者当前已提交的日志条目的最大索引
	LeaderCommit int
}

// String 方法为 AppendEntriesArgs 结构体提供了一个格式化的字符串表示，方便调试和日志记录。
func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

// AppendEntriesReply 是 AppendEntries RPC 的回复结构体，跟随者使用该结构体向领导者反馈日志追加的结果。
// 包括任期号、是否成功追加以及可能的冲突信息。
type AppendEntriesReply struct {
	// Term 是跟随者的当前任期号，用于领导者更新自己的任期
	Term int
	// Success 表示日志追加操作是否成功
	Success bool
	// ConfilictIndex 是发生日志冲突的日志条目的索引
	ConfilictIndex int
	// ConfilictTerm 是发生日志冲突的日志条目的任期号
	ConfilictTerm int
}

// String 方法为 AppendEntriesReply 结构体提供了一个格式化的字符串表示，方便调试和日志记录。
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// AppendEntries 是处理 AppendEntries RPC 请求的方法，当跟随者接收到领导者的日志追加请求时会调用此方法。
// 该方法会检查请求的合法性，处理日志追加，并更新自己的提交索引。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 加锁以保证对共享状态的安全访问
	rf.mu.Lock()
	// 确保函数结束时解锁
	defer rf.mu.Unlock()
	// 记录接收到的 AppendEntries 请求的详细信息，用于调试
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	// 初始化回复的任期号为当前节点的任期号
	reply.Term = rf.currentTerm
	// 初始化回复的成功标志为 false
	reply.Success = false

	// 处理任期号不一致的情况
	// 如果领导者的任期号小于当前节点的任期号，拒绝该请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	// 如果领导者的任期号大于等于当前节点的任期号，将当前节点转换为跟随者并更新任期
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 使用 defer 确保在函数结束时重置选举计时器，并记录冲突信息（如果追加失败）
	defer func() {
		// 重置选举计时器，防止跟随者发起不必要的选举
		rf.resetElectionTimerLocked()
		if !reply.Success {
			// 记录日志冲突信息和当前日志状态，方便调试
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// 检查前一个日志条目是否匹配
	// 如果前一个日志条目的索引超出了当前节点日志的范围，拒绝该请求
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictTerm = InvalidTerm
		reply.ConfilictIndex = rf.log.size()
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	// 如果前一个日志条目的索引小于快照的最后索引，拒绝该请求
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		return
	}
	// 如果前一个日志条目的任期号不匹配，拒绝该请求
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// 若上述检查都通过，将领导者的日志条目追加到本地日志中
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	// 持久化当前节点的日志状态，确保数据的可靠性
	rf.persistLocked()
	// 设置回复的成功标志为 true
	reply.Success = true
	// 记录成功追加日志的信息
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 处理领导者的提交索引
	// 如果领导者的提交索引大于当前节点的提交索引，更新当前节点的提交索引并通知应用循环
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		// 通知应用循环有新的日志条目已提交，可以进行应用操作
		rf.applyCond.Signal()
	}
}

// sendAppendEntries 用于向指定的节点发送 AppendEntries RPC 请求。
// 它会调用底层的 RPC 机制发送请求，并返回请求是否成功发送和接收。
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// 调用 labrpc 库的 Call 方法发送 RPC 请求
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// getMajorityIndexLocked 计算匹配索引的中位数，用于确定大多数节点已经匹配的日志条目。
// 中位数索引可以作为领导者更新提交索引的依据。
// 此方法需要在持有锁的情况下调用，以保证线程安全。
func (rf *Raft) getMajorityIndexLocked() int {
	// 创建一个临时切片，复制 matchIndex 切片的内容
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	// 对临时切片进行排序
	sort.Ints(sort.IntSlice(tmpIndexes))
	// 计算中位数的索引
	majorityIdx := (len(rf.peers) - 1) / 2
	// 记录排序后的匹配索引和中位数索引的值，用于调试
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

// startReplication 方法用于领导者启动日志复制过程，向所有跟随者发送 AppendEntries RPC 请求。
// 它会处理请求的回复，根据回复更新匹配索引和下一个要发送的索引，并更新领导者的提交索引。
// term 是当前的任期号，确保在该任期内进行复制操作。
func (rf *Raft) startReplication(term int) bool {
	// 定义一个匿名函数，用于向指定节点发送 AppendEntries 请求并处理回复
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		// 创建一个用于接收回复的结构体
		reply := &AppendEntriesReply{}
		// 向指定节点发送 AppendEntries 请求
		ok := rf.sendAppendEntries(peer, args, reply)

		// 加锁以保证对共享状态的安全访问
		rf.mu.Lock()
		// 确保函数结束时解锁
		defer rf.mu.Unlock()
		// 如果请求失败，记录日志
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		// 记录接收到的回复信息，用于调试
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// 处理任期号不一致的情况
		// 如果回复中的任期号大于当前节点的任期号，将当前节点转换为跟随者
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 检查上下文是否丢失
		// 如果当前节点不再是领导者或任期号发生变化，放弃处理该回复
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 处理日志追加失败的情况
		if !reply.Success {
			// 记录当前的下一个要发送的索引
			prevIndex := rf.nextIndex[peer]
			if reply.ConfilictTerm == InvalidTerm {
				// 如果冲突任期号无效，将下一个要发送的索引设置为冲突索引
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				// 找到冲突任期号的第一个日志条目索引
				firstIndex := rf.log.firstFor(reply.ConfilictTerm)
				if firstIndex != InvalidIndex {
					// 如果找到，将下一个要发送的索引设置为该索引
					rf.nextIndex[peer] = firstIndex
				} else {
					// 否则，将下一个要发送的索引设置为冲突索引
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// 避免无序回复导致下一个要发送的索引向前移动
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			// 计算下一个要发送的前一个日志条目的索引和任期号
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			// 记录日志，说明日志未匹配，尝试下一个前一个日志条目
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			// 记录当前领导者的日志状态，用于调试
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}

		// 处理日志追加成功的情况
		// 更新匹配索引，记录该跟随者已经匹配到的日志条目
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		// 更新下一个要发送的索引，为下一次复制做准备
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 计算匹配索引的中位数
		majorityMatched := rf.getMajorityIndexLocked()
		// 如果中位数索引大于当前的提交索引，并且该索引对应的日志条目任期号等于当前任期号
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			// 更新领导者的提交索引
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			// 通知应用循环有新的日志条目已提交，可以进行应用操作
			rf.applyCond.Signal()
		}
	}

	// 加锁以保证对共享状态的安全访问
	rf.mu.Lock()
	// 确保函数结束时解锁
	defer rf.mu.Unlock()

	// 检查上下文是否丢失
	// 如果当前节点不再是领导者或任期号发生变化，放弃启动复制过程
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	// 遍历所有节点，向每个节点发送 AppendEntries 请求或 InstallSnapshot 请求
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// 如果是当前节点自身，更新匹配索引和下一个要发送的索引
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		// 计算前一个日志条目的索引
		prevIdx := rf.nextIndex[peer] - 1
		if prevIdx < rf.log.snapLastIdx {
			// 如果前一个日志条目的索引小于快照的最后索引，发送 InstallSnapshot 请求
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			// 记录发送 InstallSnapshot 请求的信息，用于调试
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())
			// 启动一个 goroutine 发送 InstallSnapshot 请求
			go rf.installToPeer(peer, term, args)
			continue
		}

		// 计算前一个日志条目的任期号
		prevTerm := rf.log.at(prevIdx).Term
		// 创建 AppendEntries 请求的参数
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		// 记录发送 AppendEntries 请求的信息，用于调试
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		// 启动一个 goroutine 发送 AppendEntries 请求
		go replicateToPeer(peer, args)
	}

	return true
}

// replicationTicker 是一个循环函数，定期触发日志复制过程。
// 只要节点没有被停止，并且在指定的任期内，它会不断调用 startReplication 方法。
// term 是当前的任期号，确保在该任期内进行复制操作。
func (rf *Raft) replicationTicker(term int) {
	// 只要节点未停止，就持续运行
	for !rf.killed() {
		// 启动日志复制过程
		ok := rf.startReplication(term)
		// 如果复制过程启动失败，退出循环
		if !ok {
			break
		}

		// 按照预设的复制间隔休眠，避免过于频繁地发送请求

		time.Sleep(replicateInterval)
	}
}
