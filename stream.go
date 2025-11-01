/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2023-06-23 21:26:51
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2023-06-27 19:05:17
 * @FilePath: \go-replay\stream.go
 * @Description: 核心类型与端口 —— 位点、世代、恢复结论
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package replay

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// Stream 一条可恢复流的标识 | Identifies one resumable stream
type Stream string

// Epoch 流世代
//
// 历史缓冲重建时必须换新值 它的唯一用途是让"带旧位点回来的客户端"被明确拒绝，
// 而不是收到一个看起来正常的空结果
//
// [EN] Must change whenever the history buffer is rebuilt. Its only purpose is to have a client
// returning with a stale position explicitly rejected rather than receiving a plausible-looking
// empty result
type Epoch string

// Offset 流内单调递增的位点，从 1 开始；0 表示"还没有收到任何消息"
//
// [EN] A monotonically increasing position starting at 1; zero means nothing has been received
type Offset uint64

// Cursor 客户端的恢复位点 | A client's resume position
type Cursor struct {
	Epoch  Epoch
	Offset Offset
}

// IsZero 是否为初次订阅 | Whether this is a first subscription
func (c Cursor) IsZero() bool { return c.Epoch == "" && c.Offset == 0 }

// String 形如 e1:42，便于放进日志与客户端协议
//
// 用冒号分隔而不是 JSON：这个值会在每次重连的握手里传一遍，
// 短一点就少一点带宽，且肉眼可读便于排查
//
// [EN] Rendered as e1:42 for logs and the client protocol. A colon rather than JSON, since this
// value travels in every reconnect handshake, so brevity saves bandwidth and it stays legible
func (c Cursor) String() string {
	return string(c.Epoch) + ":" + strconv.FormatUint(uint64(c.Offset), 10)
}

// ParseCursor 解析 String 的输出
//
// 解析失败返回零值而不是错误：客户端送来的位点本质上是不可信输入，
// 任何畸形值都应当退化成"初次订阅"，让它走全量重取
// 这比返回错误更安全——否则一个畸形位点会让连接直接被拒，而客户端不知道该怎么改
//
// [EN] A malformed value yields the zero cursor rather than an error, because a client-supplied
// position is untrusted input and anything malformed should degrade to a first subscription and
// a full resync. That is safer than an error, which would reject the connection outright while
// leaving the client with no way to correct itself
func ParseCursor(s string) Cursor {
	i := strings.LastIndexByte(s, ':')
	if i <= 0 || i == len(s)-1 {
		return Cursor{}
	}
	n, err := strconv.ParseUint(s[i+1:], 10, 64)
	if err != nil {
		return Cursor{}
	}
	return Cursor{Epoch: Epoch(s[:i]), Offset: Offset(n)}
}

// Record 流里的一条消息 | One record in a stream
type Record struct {
	Offset  Offset
	Payload []byte
}

// Outcome 恢复结论
//
// 刻意不用 bool：恢复失败有几种不同原因，而调用方对它们的处置可以不同
// 例如 OutcomeEpochMismatch 说明存储换过代，值得告警；
// OutcomeTooOld 只说明客户端离线太久，属于正常现象
// 用 bool 会把这两件事压成同一个值，于是监控里分不开
//
// [EN] Deliberately not a bool: recovery fails for several different reasons that a caller may
// want to treat differently. An epoch mismatch means the store was rebuilt and is worth an
// alert, whereas a too-old position merely means the client was offline for a long time and is
// routine. A bool would collapse the two and make them indistinguishable in monitoring
type Outcome uint8

const (
	// OutcomeRecovered 续传成功，漏掉的消息已全部补上
	// [EN] Resumed successfully with every missed message backfilled
	OutcomeRecovered Outcome = iota
	// OutcomeFresh 初次订阅，没有历史要补
	// [EN] A first subscription with no history to backfill
	OutcomeFresh
	// OutcomeTooOld 位点已滚出保留窗口，客户端必须全量重取
	// [EN] The position has rolled out of the retention window and the client must resync
	OutcomeTooOld
	// OutcomeEpochMismatch 世代不符，历史曾被重建，客户端必须全量重取
	// [EN] The epoch does not match, the history having been rebuilt, and the client must resync
	OutcomeEpochMismatch
	// OutcomeUnknownStream 带位点访问一条没有历史的流
	// [EN] A positioned read against a stream with no history
	OutcomeUnknownStream
)

// OK 是否可以继续增量消费
//
// 只有 Recovered 与 Fresh 为真 其余三种都必须走全量重取
//
// [EN] True only for Recovered and Fresh; the other three all require a full resync
func (o Outcome) OK() bool { return o == OutcomeRecovered || o == OutcomeFresh }

// NeedsResync 是否必须全量重取 | Whether a full resync is required
func (o Outcome) NeedsResync() bool { return !o.OK() }

// String 便于日志与监控打点 | For logs and metrics
func (o Outcome) String() string {
	switch o {
	case OutcomeRecovered:
		return "recovered"
	case OutcomeFresh:
		return "fresh"
	case OutcomeTooOld:
		return "too_old"
	case OutcomeEpochMismatch:
		return "epoch_mismatch"
	case OutcomeUnknownStream:
		return "unknown_stream"
	}
	return "unknown"
}

// ErrClosed 存储已关闭 | The store is closed
var ErrClosed = errors.New("replay: 存储已关闭")

// Appender 写入侧 | The write side
type Appender interface {
	// Append 追加一条并返回它的位点
	// [EN] Appends one record and returns its position
	Append(ctx context.Context, s Stream, payload []byte) (Cursor, error)
}

// Resumer 读取侧
//
// 与写入侧分开，是为了让只读副本只实现一半——补发历史是读多写少的操作，
// 常常由副本承担
//
// [EN] Split from the write side so a read replica implements only half: backfilling is
// read-heavy and often served by a replica
type Resumer interface {
	// Since 取 from 之后的消息，并给出恢复结论
	//
	// 约定：Outcome.NeedsResync() 为真时 records 必须为空——
	// 返回"部分消息 + 需要重取"会诱导调用方把那部分当成完整增量
	//
	// [EN] Returns records after from along with the outcome. By contract, records is empty
	// whenever the outcome needs a resync: returning partial records alongside a resync signal
	// would invite callers to treat the partial set as a complete increment
	Since(ctx context.Context, s Stream, from Cursor, limit int) (records []Record, outcome Outcome, err error)
}

// Store 读写合一，便于单实现装配 | Read and write together for a single implementation
type Store interface {
	Appender
	Resumer
}
