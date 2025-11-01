/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2023-06-23 19:21:08
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2023-06-26 20:58:33
 * @FilePath: \go-replay\doc.go
 * @Description: 包说明 —— 可恢复流的语义边界与参照来源
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

// Package replay 提供按位点重放的可恢复消息流
//
// # 它解决什么
//
// 客户端断线重连后，怎么知道自己漏了哪些消息、怎么把它们补回来，
// 以及——最关键的——**怎么知道自己补不回来了**
//
// 第三点是多数自制方案漏掉的那一点，而漏掉它的后果是客户端静默偏离：
// 它以为自己是最新的，实际上中间有一段永远不会到达，且没有任何信号提示这件事
//
// # 形状不是自创
//
// 这套"位点 + 有界历史 + 明确的恢复失败信号"来自几套已被验证的实现：
//
//   - Centrifugo 的 recovery：每频道一个单调 offset 加一个 epoch，
//     客户端带位点重连，服务端在窗口内补发，补不上就明确告知失败
//   - Redis Streams 消费组：XADD 分配 ID、XREADGROUP 按位点读、PEL 跟踪未确认
//   - NATS JetStream durable consumer、Kafka consumer offset：
//     都是"位点 + 可重放日志 + 保留策略"
//
// 本库把这个形状抽成两个方法的端口，并给一个零依赖的内存实现作为参照语义
//
// # epoch 为什么不能省
//
// 只有 offset 时存在一个会静默偏离的洞：历史缓冲本身可能丢失
// （存储重启、节点换人、缓冲被清空）此时客户端带着 offset=1000 回来，
// 而服务端历史是空的，于是"1000 之后没有新消息"与"1000 之后全丢了"
// 在接口上长得一模一样
//
// 给每条流一个 epoch，缓冲重建时换新值 客户端带回的 epoch 与当前不符即判定
// 历史不连续，明确返回恢复失败，让客户端走全量重取
//
// # 非目标
//
//   - 不提供传输：本库只管"位点与历史"，消息怎么送到客户端由调用方决定
//   - 不保证消息不丢：窗口之外的消息就是丢了，本库保证的是**你会知道它丢了**
//   - 不做去重：至少一次投递下的重复由调用方按业务幂等键处理
//   - 不做跨流事务：每条流各自独立，没有跨流的顺序或原子性保证
//
// [EN] Package replay provides resumable message streams addressed by position. It answers how
// a reconnecting client learns which messages it missed, how to backfill them and — most
// importantly — how it learns that backfilling is impossible. That third point is the one most
// hand-rolled designs omit, and omitting it makes clients diverge silently: they believe they
// are current while a gap will never arrive and nothing signals it.
//
// The shape of a position plus a bounded history plus an explicit recovery-failure signal is
// not invented here; it follows Centrifugo's recovery, Redis Streams consumer groups, NATS
// JetStream durable consumers and Kafka consumer offsets. This library distils that into a
// two-method port with a dependency-free in-memory implementation as the reference semantics.
//
// The epoch cannot be omitted: a history buffer can itself be lost, and with only an offset
// "nothing new after 1000" is indistinguishable from "everything after 1000 was lost". An epoch
// that changes on rebuild turns that into an explicit failure.
//
// Non-goals: no transport, since this library only owns positions and history; no guarantee
// against loss, since messages outside the window are genuinely gone and what is guaranteed is
// that you will know; no deduplication, as duplicates under at-least-once delivery are the
// caller's to handle with a business idempotency key; and no cross-stream transactions, each
// stream being independent with no ordering or atomicity across them.
package replay
