/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2023-06-26 22:18:33
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2023-06-28 21:51:20
 * @FilePath: \go-replay\replay_test.go
 * @Description: 恢复语义测试 —— 只用标准库，保持零依赖
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package replay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newStore(t *testing.T, cfg Config) *MemoryStore {
	t.Helper()
	if cfg.Epoch == "" {
		cfg.Epoch = "e1"
	}
	return NewMemoryStore(cfg)
}

// 窗口内续传：补上漏掉的那几条，且位点连续
func TestResumeWithinWindow(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{MaxPerStream: 100})
	const st Stream = "room-1"

	var at3 Cursor
	for i := 1; i <= 10; i++ {
		c, err := s.Append(ctx, st, []byte(fmt.Sprintf("m%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			at3 = c
		}
	}

	recs, out, err := s.Since(ctx, st, at3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out != OutcomeRecovered || !out.OK() {
		t.Fatalf("应为 recovered，得到 %v", out)
	}
	if len(recs) != 7 {
		t.Fatalf("应补 7 条，得到 %d", len(recs))
	}
	for i, r := range recs {
		wantOff := Offset(i + 4)
		if r.Offset != wantOff {
			t.Errorf("第 %d 条位点=%d 期望 %d —— 位点必须连续", i, r.Offset, wantOff)
		}
	}
}

// 位点滚出窗口：必须明确 TooOld，且不返回部分消息
func TestResumeTooOld(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{MaxPerStream: 5})
	const st Stream = "room-1"

	first, _ := s.Append(ctx, st, []byte("m1"))
	for i := 2; i <= 20; i++ {
		_, _ = s.Append(ctx, st, []byte(fmt.Sprintf("m%d", i)))
	}

	recs, out, err := s.Since(ctx, st, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out != OutcomeTooOld {
		t.Errorf("应为 too_old，得到 %v", out)
	}
	if !out.NeedsResync() {
		t.Error("too_old 必须要求全量重取")
	}
	if len(recs) != 0 {
		t.Errorf("要求重取时不得返回部分消息，得到 %d 条 —— 否则调用方会把它当成完整增量", len(recs))
	}
}

// 这是最容易漏掉的一条：世代变了必须拒绝旧位点
func TestEpochMismatchForcesResync(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{MaxPerStream: 100, Epoch: "epoch-A"})
	const st Stream = "room-1"

	at, _ := s.Append(ctx, st, []byte("m1"))
	_, _ = s.Append(ctx, st, []byte("m2"))

	s.Rebuild("epoch-B")
	_, _ = s.Append(ctx, st, []byte("m3"))

	recs, out, err := s.Since(ctx, st, at, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out != OutcomeEpochMismatch {
		t.Errorf("应为 epoch_mismatch，得到 %v —— 漏掉这条客户端会静默偏离", out)
	}
	if len(recs) != 0 {
		t.Errorf("世代不符时不得返回消息，得到 %d 条", len(recs))
	}
}

// 世代不符优先于位点太老：前者更根本，监控上也该分开
func TestEpochCheckedBeforeTooOld(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{MaxPerStream: 2, Epoch: "A"})
	const st Stream = "s"
	for i := 0; i < 10; i++ {
		_, _ = s.Append(ctx, st, []byte("x"))
	}
	// 位点既太老、世代也不符
	_, out, _ := s.Since(ctx, st, Cursor{Epoch: "B", Offset: 1}, 0)
	if out != OutcomeEpochMismatch {
		t.Errorf("世代不符应优先于太老，得到 %v", out)
	}
}

// 初次订阅算 Fresh，不补历史
func TestFirstSubscribeIsFresh(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{})
	recs, out, err := s.Since(ctx, "any", Cursor{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out != OutcomeFresh || !out.OK() {
		t.Errorf("应为 fresh，得到 %v", out)
	}
	if len(recs) != 0 {
		t.Errorf("初次订阅不该补历史，得到 %d 条", len(recs))
	}
}

// 带位点访问无历史的流：UnknownStream
func TestPositionedOnUnknownStream(t *testing.T) {
	s := newStore(t, Config{})
	_, out, err := s.Since(context.Background(), "never", Cursor{Epoch: "e1", Offset: 5}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out != OutcomeUnknownStream {
		t.Errorf("应为 unknown_stream，得到 %v", out)
	}
}

// 按时长淘汰
//
// 要点：客户端位点那条被淘汰**不构成缺口**——它已经收到过那条了，要的是之后的
// 真正的缺口是"位点之后的消息也被淘汰了"，所以这里让 m1 与 m2 一起过期，
// 客户端停在 m1，于是它要的 m2 确实没了，必须判 too_old
func TestEvictByAge(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	s := newStore(t, Config{MaxPerStream: 1000, MaxAge: time.Minute, Now: func() time.Time { return now }})
	const st Stream = "s"

	at1, _ := s.Append(ctx, st, []byte("m1"))
	_, _ = s.Append(ctx, st, []byte("m2"))

	now = now.Add(2 * time.Minute) // m1 与 m2 一起过期
	_, _ = s.Append(ctx, st, []byte("m3"))

	_, out, _ := s.Since(ctx, st, at1, 0)
	if out != OutcomeTooOld {
		t.Errorf("位点之后的消息已过期，应为 too_old，得到 %v", out)
	}
	if ev := s.Stats().Evicted; ev < 2 {
		t.Errorf("应淘汰至少 2 条，得到 %d", ev)
	}
}

// 对照：只有位点那一条被淘汰时，不算缺口，应正常续传
//
// 单独立一个测试把这条语义钉住，否则很容易在"修"上一个测试时把判定改松
func TestEvictingTheCursorRecordIsNotAGap(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	s := newStore(t, Config{MaxPerStream: 1000, MaxAge: time.Minute, Now: func() time.Time { return now }})
	const st Stream = "s"

	at1, _ := s.Append(ctx, st, []byte("m1"))
	now = now.Add(2 * time.Minute) // 只有 m1 过期
	_, _ = s.Append(ctx, st, []byte("m2"))

	recs, out, _ := s.Since(ctx, st, at1, 0)
	if out != OutcomeRecovered {
		t.Fatalf("客户端要的是 m2，它还在窗口里，应为 recovered，得到 %v", out)
	}
	if len(recs) != 1 || string(recs[0].Payload) != "m2" {
		t.Errorf("应补发 m2，得到 %v", recs)
	}
}

// 按条数与按时长取先到者
func TestEvictCountAndAgeWhicheverFirst(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	s := newStore(t, Config{MaxPerStream: 3, MaxAge: time.Hour, Now: func() time.Time { return now }})
	const st Stream = "s"
	for i := 0; i < 10; i++ {
		_, _ = s.Append(ctx, st, []byte("x"))
	}
	// 时长远未到，但条数已超，应只留 3 条
	recs, out, _ := s.Since(ctx, st, Cursor{Epoch: "e1", Offset: 7}, 0)
	if out != OutcomeRecovered {
		t.Fatalf("应可恢复，得到 %v", out)
	}
	if len(recs) != 3 {
		t.Errorf("条数上限 3，应留 3 条，得到 %d", len(recs))
	}
}

// limit 生效，避免一次补发压垮慢客户端
func TestSinceRespectsLimit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{MaxPerStream: 200})
	const st Stream = "s"
	for i := 0; i < 50; i++ {
		_, _ = s.Append(ctx, st, []byte("x"))
	}
	recs, out, _ := s.Since(ctx, st, Cursor{Epoch: "e1", Offset: 0}, 10)
	if out != OutcomeRecovered {
		t.Fatalf("应可恢复，得到 %v", out)
	}
	if len(recs) != 10 {
		t.Errorf("limit=10 应返回 10 条，得到 %d", len(recs))
	}
}

// 载荷必须被复制：调用方复用缓冲不能污染历史
func TestAppendCopiesPayload(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{})
	const st Stream = "s"

	buf := []byte("original")
	c, _ := s.Append(ctx, st, buf)
	copy(buf, "MUTATED!")

	recs, _, _ := s.Since(ctx, st, Cursor{Epoch: "e1", Offset: c.Offset - 1}, 0)
	if len(recs) != 1 {
		t.Fatalf("应有 1 条，得到 %d", len(recs))
	}
	if string(recs[0].Payload) != "original" {
		t.Errorf("历史被调用方的缓冲改写了: %q", recs[0].Payload)
	}
}

// Cursor 的字符串往返；畸形输入退化成零值而不是报错
func TestCursorStringRoundTrip(t *testing.T) {
	for _, c := range []Cursor{
		{Epoch: "e1", Offset: 42},
		{Epoch: "m20261031235959.123456789", Offset: 1},
		{Epoch: "has:colon", Offset: 7},
	} {
		if got := ParseCursor(c.String()); got != c {
			t.Errorf("往返失败: %v → %q → %v", c, c.String(), got)
		}
	}
	for _, bad := range []string{"", ":", "e1:", ":5", "e1", "e1:abc", "e1:-1"} {
		if got := ParseCursor(bad); !got.IsZero() {
			t.Errorf("畸形输入 %q 应退化成零值，得到 %v —— 客户端位点是不可信输入", bad, got)
		}
	}
}

// Latest 给首订客户端一个基准
func TestLatest(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{})
	if _, ok := s.Latest("none"); ok {
		t.Error("不存在的流应返回 false")
	}
	const st Stream = "s"
	var last Cursor
	for i := 0; i < 5; i++ {
		last, _ = s.Append(ctx, st, []byte("x"))
	}
	got, ok := s.Latest(st)
	if !ok || got != last {
		t.Errorf("Latest()=%v,%v 期望 %v,true", got, ok, last)
	}
}

// 关闭后拒绝读写
func TestClosed(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{})
	_ = s.Close()
	if _, err := s.Append(ctx, "s", []byte("x")); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Append 应返回 ErrClosed，得到 %v", err)
	}
	if _, _, err := s.Since(ctx, "s", Cursor{}, 0); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Since 应返回 ErrClosed，得到 %v", err)
	}
}

// Outcome 的 OK/NeedsResync 划分必须互补
func TestOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		o  Outcome
		ok bool
	}{
		{OutcomeRecovered, true}, {OutcomeFresh, true},
		{OutcomeTooOld, false}, {OutcomeEpochMismatch, false}, {OutcomeUnknownStream, false},
	} {
		if tc.o.OK() != tc.ok {
			t.Errorf("%v.OK()=%v 期望 %v", tc.o, tc.o.OK(), tc.ok)
		}
		if tc.o.NeedsResync() == tc.ok {
			t.Errorf("%v 的 OK 与 NeedsResync 应互补", tc.o)
		}
		if tc.o.String() == "unknown" {
			t.Errorf("%d 缺 String 映射", tc.o)
		}
	}
}

// 并发读写无竞争（配 -race）
func TestConcurrent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Config{MaxPerStream: 64, MaxAge: time.Minute})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			st := Stream(fmt.Sprintf("s%d", g%4))
			for i := 0; i < 300; i++ {
				c, err := s.Append(ctx, st, []byte("x"))
				if err != nil {
					t.Error(err)
					return
				}
				if _, _, err := s.Since(ctx, st, c, 10); err != nil {
					t.Error(err)
					return
				}
				_, _ = s.Latest(st)
			}
		}(g)
	}
	wg.Wait()
	if st := s.Stats(); st.Appended != 16*300 {
		t.Errorf("Appended=%d 期望 %d", st.Appended, 16*300)
	}
}
