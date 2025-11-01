/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2023-06-25 20:51:19
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2023-06-27 22:12:06
 * @FilePath: \go-replay\memory.go
 * @Description: 内存实现 —— 按条数与时长双重保留，兼作适配器的参照语义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package replay

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// 为什么保留策略要同时按条数和按时长
//
// 只按条数：一条冷流（每天几条）会把几个月前的消息一直留着，
// 客户端"恢复成功"地收到一批早已无意义的旧消息
// 只按时长：一条热流（每秒上千条）在窗口内能堆出天文数字，内存失控
//
// 两者取**先到者**：任一条件触发即淘汰 这与 Kafka 的 retention.bytes +
// retention.ms、Redis Streams 的 MAXLEN + MINID 是同一个思路
//
// [EN] Count-only retention lets a cold stream of a few records a day keep months-old messages,
// so a client "recovers" a batch that is long meaningless. Time-only retention lets a hot
// stream of a thousand records a second pile up astronomically inside the window and lose
// control of memory. Whichever triggers first wins, the same approach as Kafka's retention
// bytes and milliseconds or Redis Streams' MAXLEN with MINID.

// Config 内存存储的配置 | Configuration for the in-memory store
type Config struct {
	// MaxPerStream 每条流保留的最大条数，<=0 用默认 256
	// [EN] Maximum records retained per stream; the default of 256 applies when non-positive
	MaxPerStream int

	// MaxAge 每条消息的最长保留时长，<=0 表示不按时长淘汰
	// [EN] Maximum age of a record; non-positive disables time-based eviction
	MaxAge time.Duration

	// Epoch 本次启动的流世代
	//
	// **进程重启后必须换新值**，否则客户端会拿着上一世的位点来续传，
	// 而内存历史早已空了，于是收到"恢复成功 + 零条消息"——静默偏离
	// 留空时由 NewMemoryStore 按启动时间自动生成
	//
	// [EN] Must change after a restart, or a client resumes with a position from the previous
	// incarnation against an empty history and receives a successful recovery with no records,
	// which is a silent divergence. When empty, NewMemoryStore derives one from the start time
	Epoch Epoch

	// Now 取当前时间，供测试注入；为 nil 时用 time.Now
	// [EN] Injectable clock for tests; time.Now when nil
	Now func() time.Time
}

// MemoryStore 进程内的可恢复流存储
//
// 按流分片，因为不同流之间完全独立，没有任何跨流不变量要维护，
// 单锁只会让互不相干的流互相等
//
// [EN] An in-process store sharded by stream, since streams are entirely independent with no
// cross-stream invariant to maintain and one lock would only make unrelated streams wait
type MemoryStore struct {
	cfg    Config
	now    func() time.Time
	shards [storeShards]storeShard

	appended atomic.Uint64
	evicted  atomic.Uint64
	resyncs  atomic.Uint64

	closed atomic.Bool
}

const storeShards = 32

type storeShard struct {
	mu sync.RWMutex
	m  map[Stream]*log
}

type entry struct {
	offset  Offset
	at      time.Time
	payload []byte
}

type log struct {
	items []entry
	next  Offset // 下一个要分配的位点
}

// NewMemoryStore 建内存存储
//
// Epoch 留空时按启动时间生成，形如 "m1762000000123456789"：
// 这样进程重启自然换代，不必调用方记得去改
//
// [EN] An empty epoch is derived from the start time so a restart changes generation naturally
// without the caller having to remember
func NewMemoryStore(cfg Config) *MemoryStore {
	if cfg.MaxPerStream <= 0 {
		cfg.MaxPerStream = 256
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Epoch == "" {
		cfg.Epoch = Epoch("m" + time.Now().Format("20060102150405.000000000"))
	}
	s := &MemoryStore{cfg: cfg, now: cfg.Now}
	for i := range s.shards {
		s.shards[i].m = make(map[Stream]*log)
	}
	return s
}

// fnv1a 内联字符串哈希，避免引 hash/fnv 并为每次取模分配一个 hasher
// [EN] Inlined to avoid a hash/fnv import and a per-call hasher allocation
func fnv1a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (s *MemoryStore) shardOf(st Stream) *storeShard {
	return &s.shards[fnv1a(string(st))&(storeShards-1)]
}

// Epoch 当前流世代 | The current epoch
func (s *MemoryStore) Epoch() Epoch { return s.cfg.Epoch }

// Append 实现 Appender | Implements Appender
func (s *MemoryStore) Append(_ context.Context, st Stream, payload []byte) (Cursor, error) {
	if s.closed.Load() {
		return Cursor{}, ErrClosed
	}
	// 复制载荷：调用方常常复用缓冲，不复制的话历史里的内容会被后续写入改掉
	// [EN] The payload is copied because callers commonly reuse buffers, and without a copy a
	// later write would mutate what the history holds
	cp := make([]byte, len(payload))
	copy(cp, payload)

	now := s.now()
	sh := s.shardOf(st)

	sh.mu.Lock()
	lg := sh.m[st]
	if lg == nil {
		lg = &log{items: make([]entry, 0, min(s.cfg.MaxPerStream, 16)), next: 1}
		sh.m[st] = lg
	}
	off := lg.next
	lg.next++
	lg.items = append(lg.items, entry{offset: off, at: now, payload: cp})
	n := s.evictLocked(lg, now)
	sh.mu.Unlock()

	s.appended.Add(1)
	if n > 0 {
		s.evicted.Add(uint64(n))
	}
	return Cursor{Epoch: s.cfg.Epoch, Offset: off}, nil
}

// evictLocked 按条数与时长淘汰，返回淘汰条数；调用方必须持锁
func (s *MemoryStore) evictLocked(lg *log, now time.Time) int {
	cut := 0
	// 按条数
	if over := len(lg.items) - s.cfg.MaxPerStream; over > 0 {
		cut += over
	}
	// 按时长：items 按时间递增，从头扫到第一条未过期为止
	if s.cfg.MaxAge > 0 {
		limit := now.Add(-s.cfg.MaxAge)
		for cut < len(lg.items) && lg.items[cut].at.Before(limit) {
			cut++
		}
	}
	if cut > 0 {
		// 整段前移而不是逐条 delete：切片前移是一次 memmove
		// [EN] One memmove rather than per-item deletion
		lg.items = append(lg.items[:0], lg.items[cut:]...)
	}
	return cut
}

// Since 实现 Resumer | Implements Resumer
func (s *MemoryStore) Since(_ context.Context, st Stream, from Cursor, limit int) ([]Record, Outcome, error) {
	if s.closed.Load() {
		return nil, OutcomeEpochMismatch, ErrClosed
	}
	// 世代不符优先判：它说明历史不连续，比"位点太老"更根本
	// [EN] Checked first because a broken generation is more fundamental than a stale position
	if !from.IsZero() && from.Epoch != s.cfg.Epoch {
		s.resyncs.Add(1)
		return nil, OutcomeEpochMismatch, nil
	}

	sh := s.shardOf(st)
	sh.mu.RLock()
	lg := sh.m[st]
	if lg == nil {
		sh.mu.RUnlock()
		if from.IsZero() {
			return nil, OutcomeFresh, nil
		}
		s.resyncs.Add(1)
		return nil, OutcomeUnknownStream, nil
	}
	if from.IsZero() {
		sh.mu.RUnlock()
		return nil, OutcomeFresh, nil
	}
	// 位点滚出窗口：窗口内最老的一条是 items[0]，
	// 客户端要的是 from.Offset 的**下一条**，所以可恢复的条件是 oldest <= from+1
	// [EN] The client wants the record after from.Offset, so recovery requires the oldest
	// retained offset to be at most from.Offset+1
	if len(lg.items) > 0 && lg.items[0].offset > from.Offset+1 {
		sh.mu.RUnlock()
		s.resyncs.Add(1)
		return nil, OutcomeTooOld, nil
	}
	out := make([]Record, 0, 8)
	for i := range lg.items {
		if lg.items[i].offset <= from.Offset {
			continue
		}
		out = append(out, Record{Offset: lg.items[i].offset, Payload: lg.items[i].payload})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	sh.mu.RUnlock()
	return out, OutcomeRecovered, nil
}

// Latest 流的最新位点，供客户端首次订阅时取基准
//
// 不存在的流返回零值 Cursor 与 false
//
// [EN] The newest position, for a client taking a baseline on first subscription; an unknown
// stream yields the zero cursor and false
func (s *MemoryStore) Latest(st Stream) (Cursor, bool) {
	sh := s.shardOf(st)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	lg := sh.m[st]
	if lg == nil || len(lg.items) == 0 {
		return Cursor{}, false
	}
	return Cursor{Epoch: s.cfg.Epoch, Offset: lg.items[len(lg.items)-1].offset}, true
}

// Rebuild 换新世代并清空
//
// 用于"检测到底层存储重启"时主动换代 换代后所有带位点的客户端都会收到
// OutcomeEpochMismatch，从而走全量重取——这正是期望行为
//
// [EN] Adopts a new epoch and clears, for use when an underlying store restart is detected.
// Every positioned client then receives an epoch mismatch and resyncs, which is intended
func (s *MemoryStore) Rebuild(e Epoch) {
	for i := range s.shards {
		s.shards[i].mu.Lock()
		s.shards[i].m = make(map[Stream]*log)
		s.shards[i].mu.Unlock()
	}
	s.cfg.Epoch = e
}

// Close 关闭；之后 Append 与 Since 返回 ErrClosed
// [EN] Closes; subsequent calls return ErrClosed
func (s *MemoryStore) Close() error {
	s.closed.Store(true)
	return nil
}

// Stats 运行统计 | Runtime statistics
type Stats struct {
	// Appended 累计追加条数 | Records appended
	Appended uint64
	// Evicted 累计淘汰条数 | Records evicted
	Evicted uint64
	// Resyncs 累计要求全量重取的次数
	//
	// 这个数值得盯：持续不为零说明保留窗口对实际的离线时长而言太小
	//
	// [EN] Worth watching: a persistently non-zero value means the retention window is too
	// small for how long clients actually stay offline
	Resyncs uint64
	// Streams 当前流数量 | Current stream count
	Streams int
}

// Stats 取统计 | Returns statistics
func (s *MemoryStore) Stats() Stats {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.RLock()
		n += len(s.shards[i].m)
		s.shards[i].mu.RUnlock()
	}
	return Stats{
		Appended: s.appended.Load(),
		Evicted:  s.evicted.Load(),
		Resyncs:  s.resyncs.Load(),
		Streams:  n,
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ Store = (*MemoryStore)(nil)
