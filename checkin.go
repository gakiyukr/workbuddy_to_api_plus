package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const checkinStateFile = "checkin-state.json"

var checkinZone = time.FixedZone("UTC+8", 8*60*60)
var dailyCheckins = newDailyCheckinScheduler(checkinStateFile)

type checkinStatusSnapshot struct {
	Date     string `json:"date"`
	Status   string `json:"status"` // pending/running/success/failed/missed/unsupported/unknown
	Attempts int    `json:"attempts"`
	NextAt   int64  `json:"nextAt,omitempty"`
	LastAt   int64  `json:"lastAt,omitempty"`
	Message  string `json:"message,omitempty"`
}

// Only hashes and fixed status messages are saved; never save UID/token/nickname.
type checkinRecord struct {
	FirstAt   int64  `json:"firstAt"`
	RetryAt   int64  `json:"retryAt"`
	StartupAt int64  `json:"startupAt,omitempty"`
	Attempts  int    `json:"attempts"`
	Status    string `json:"status"`
	LastAt    int64  `json:"lastAt,omitempty"`
	Message   string `json:"message,omitempty"`
	RetryEnd  bool   `json:"retryEnd,omitempty"`
}

type checkinDocument struct {
	Schema        int                       `json:"schema"`
	Date          string                    `json:"date"`
	Plans         map[string]*checkinRecord `json:"plans"`
	LastStartupAt int64                     `json:"lastStartupAt,omitempty"`
}

type checkinTarget struct {
	key       string
	account   *Account
	supported bool
	eligible  bool
}

type checkinJob struct {
	key     string
	date    string
	attempt int
	end     time.Time
	account *Account
	trace   string
	started time.Time
}

type checkinExpectedIdentity struct{}

type dailyCheckinScheduler struct {
	mu       sync.Mutex
	file     string
	loaded   bool
	blocked  bool
	doc      checkinDocument
	active   map[string]bool
	lastNext int64
	now      func() time.Time
	random   func(time.Duration) (time.Duration, error)
	perform  func(context.Context, *Account, string) (string, error)
}

func newDailyCheckinScheduler(file string) *dailyCheckinScheduler {
	return &dailyCheckinScheduler{file: file, now: time.Now, random: randomCheckinOffset,
		perform: checkinAccountWithTrace, active: map[string]bool{}}
}

func randomCheckinOffset(window time.Duration) (time.Duration, error) {
	// Whole-second targets match the scheduler's one-second polling cadence;
	// never choose the final fractional millisecond immediately before cutoff.
	n, err := rand.Int(rand.Reader, big.NewInt(int64(window/time.Second)))
	if err != nil {
		return 0, err
	}
	return time.Duration(n.Int64()) * time.Second, nil
}

func checkinWindows(now time.Time) (time.Time, time.Time, time.Time) {
	n := now.In(checkinZone)
	start := time.Date(n.Year(), n.Month(), n.Day(), 8, 0, 0, 0, checkinZone)
	return start, start.Add(90 * time.Minute), start.Add(2 * time.Hour)
}

func checkinDay(now time.Time) string { return now.In(checkinZone).Format("2006-01-02") }

// Caller holds accountMu. Stable across credential refresh/rename when UID exists.
func checkinTargetLocked(acc *Account) checkinTarget {
	if acc == nil {
		return checkinTarget{}
	}
	uid, enterprise := acc.UID, ""
	if acc.Auth != nil {
		uid, enterprise = acc.Auth.Account.UID, acc.Auth.Account.EnterpriseID
	}
	identity := "uid:" + uid
	if uid == "" {
		absolute, _ := filepath.Abs(acc.Path)
		identity = "file:" + filepath.Clean(absolute)
	}
	digest := sha256.Sum256([]byte(acc.Profile().Key + ":" + identity))
	supported := acc.Profile().Key == profileCN.Key && strings.TrimSpace(enterprise) == ""
	return checkinTarget{key: hex.EncodeToString(digest[:]), account: acc, supported: supported,
		eligible: supported && !acc.Disabled && acc.Auth != nil && acc.Auth.Auth.AccessToken != ""}
}

func currentCheckinTargets() []checkinTarget {
	accountMu.Lock()
	defer accountMu.Unlock()
	result := make([]checkinTarget, 0, len(accounts))
	for _, acc := range accounts {
		result = append(result, checkinTargetLocked(acc))
	}
	return result
}

func (s *dailyCheckinScheduler) loadLocked() {
	if s.loaded {
		return
	}
	s.loaded = true
	s.doc = checkinDocument{Schema: 1, Plans: map[string]*checkinRecord{}}
	info, err := os.Lstat(s.file)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() > 8<<20 {
		s.blockLocked("状态文件不可读或不是受支持的普通文件")
		return
	}
	raw, err := os.ReadFile(s.file)
	var doc checkinDocument
	if err != nil || json.Unmarshal(raw, &doc) != nil || doc.Schema != 1 || doc.Plans == nil {
		s.blockLocked("状态文件损坏；原文件已保留")
		return
	}
	day, err := time.ParseInLocation("2006-01-02", doc.Date, checkinZone)
	if err != nil {
		s.blockLocked("状态文件日期无效；原文件已保留")
		return
	}
	start, retry, end := checkinWindows(day)
	dayEnd := start.Add(16 * time.Hour)
	if doc.LastStartupAt != 0 && (doc.LastStartupAt < start.UnixMilli() || doc.LastStartupAt >= dayEnd.UnixMilli()) {
		s.blockLocked("启动补签间隔记录无效；原文件已保留")
		return
	}
	for key, p := range doc.Plans {
		_, keyErr := hex.DecodeString(key)
		validState := p != nil && (p.Status == "pending" || p.Status == "running" ||
			p.Status == "success" || p.Status == "failed" || p.Status == "missed")
		if keyErr != nil || len(key) != 64 || !validState || p.Attempts < 0 || p.Attempts > 2 ||
			p.FirstAt < start.UnixMilli() || p.FirstAt >= retry.UnixMilli() ||
			p.RetryAt < retry.UnixMilli() || p.RetryAt >= end.UnixMilli() ||
			(p.StartupAt != 0 && (p.StartupAt < start.UnixMilli() || p.StartupAt >= dayEnd.UnixMilli())) ||
			(p.Status == "success" && p.Attempts == 0) {
			s.blockLocked("状态文件记录无效；为防止重复请求暂停签到")
			return
		}
	}
	s.doc = doc
	log.Printf("[签到状态] traceId=%s 阶段=读取 结果=成功 日期=%s 记录=%d 说明=仅恢复状态，不发送签到", newTraceID(), doc.Date, len(doc.Plans))
}

func (s *dailyCheckinScheduler) blockLocked(reason string) {
	s.blocked = true
	log.Printf("[签到状态] traceId=%s 阶段=持久化保护 结果=暂停签到 原因=%s 说明=本进程不再发签到请求，不覆盖原状态文件", newTraceID(), reason)
}

func (s *dailyCheckinScheduler) saveLocked(trace string) bool {
	if s.blocked {
		return false
	}
	if info, err := os.Lstat(s.file); err == nil && !info.Mode().IsRegular() || err != nil && !errors.Is(err, os.ErrNotExist) {
		s.blockLocked("写入目标不是普通文件或无法检查")
		return false
	}
	raw, err := json.MarshalIndent(s.doc, "", "  ")
	if err != nil {
		s.blockLocked("状态序列化失败")
		return false
	}
	f, err := os.CreateTemp(filepath.Dir(s.file), ".checkin-state-*.tmp")
	if err != nil {
		s.blockLocked("无法创建状态临时文件")
		return false
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	err = f.Chmod(0600)
	if err == nil {
		_, err = f.Write(append(raw, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, s.file)
	}
	if err != nil {
		s.blockLocked("状态无法可靠落盘；未发送新的签到")
		return false
	}
	log.Printf("[签到状态] traceId=%s 阶段=原子落盘 日期=%s 记录=%d 结果=成功 说明=先保存尝试次数，随后才允许请求上游", trace, s.doc.Date, len(s.doc.Plans))
	return true
}

// Startup after 08:00 queues only pending, never-attempted accounts. The first
// runs immediately; subsequent requests are spaced at least one minute apart.
// Credential hot reloads do not create a startup queue.
func (s *dailyCheckinScheduler) planAndClaim(now time.Time, targets []checkinTarget, startup bool) []checkinJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	if s.blocked {
		return nil
	}
	day := checkinDay(now)
	if s.doc.Date > day {
		s.blockLocked("北京时间早于已保存日期；等待校准时钟后重启")
		return nil
	}
	dirty := false
	if s.doc.Date != day {
		s.doc = checkinDocument{Schema: 1, Date: day, Plans: map[string]*checkinRecord{}}
		dirty = true
	}
	start, retry, end := checkinWindows(now)
	dayEnd := start.Add(16 * time.Hour)
	if startup {
		// Recover every interrupted record, including accounts removed from the
		// current pool, so an old in-flight marker cannot stall the whole queue.
		for _, p := range s.doc.Plans {
			if p.Status != "running" {
				continue
			}
			p.Status, p.Message = "failed", "上次尝试未能确认结果；不补发原请求"
			if p.StartupAt != 0 && !now.Before(start) {
				s.doc.LastStartupAt = max(s.doc.LastStartupAt, now.UnixMilli())
			}
			if p.Attempts == 2 {
				p.RetryEnd = true
				p.Message += "；今日已用完两次尝试"
			}
			dirty = true
		}
	}
	s.active = map[string]bool{}
	var jobs []checkinJob
	var eligible []checkinTarget
	for _, target := range targets {
		if !target.eligible || s.active[target.key] {
			continue
		}
		s.active[target.key] = true
		eligible = append(eligible, target)
		p := s.doc.Plans[target.key]
		if p == nil {
			firstOffset, e1 := s.random(retry.Sub(start))
			retryOffset, e2 := s.random(end.Sub(retry))
			if e1 != nil || e2 != nil || firstOffset < 0 || firstOffset >= retry.Sub(start) ||
				retryOffset < 0 || retryOffset >= end.Sub(retry) {
				s.blockLocked("无法生成有效随机计划")
				return nil
			}
			p = &checkinRecord{FirstAt: start.Add(firstOffset).UnixMilli(), RetryAt: retry.Add(retryOffset).UnixMilli(), Status: "pending"}
			s.doc.Plans[target.key] = p
			log.Printf("[签到计划] traceId=%s 阶段=生成 日期=%s 账号标识=%s 首次=%s 失败重试=%s 说明=北京时间；后续重启不重新随机",
				newTraceID(), day, target.key[:12], time.UnixMilli(p.FirstAt).In(checkinZone).Format("15:04:05"),
				time.UnixMilli(p.RetryAt).In(checkinZone).Format("15:04:05"))
			dirty = true
			if p.FirstAt <= now.UnixMilli() && !(startup && !now.Before(start)) {
				p.Status, p.Message = "missed", "今日随机签到时间已过；账号事件不触发补签"
			}
		}
		if p.Status == "failed" && p.Attempts == 1 && !p.RetryEnd &&
			(!now.Before(end) || startup && p.RetryAt <= now.UnixMilli() ||
				p.StartupAt != 0 && max(p.RetryAt, s.startupDueLocked(p)) >= end.UnixMilli()) {
			p.RetryEnd, dirty = true, true
			p.Message = "今日失败重试时间已过；不补签，当天不再重试"
		}
		if p.Status == "pending" && p.StartupAt == 0 && !now.Before(retry) && !startup {
			p.Status, p.Message, dirty = "missed", "今日首次签到窗口已过；不补签", true
		}
		if p.Status == "pending" && p.StartupAt != 0 && s.startupDueLocked(p) >= dayEnd.UnixMilli() {
			p.Status, p.Message, dirty = "missed", "今日剩余时间不足以保持一分钟间隔；不跨日补签", true
		}
	}
	if startup && !now.Before(start) {
		if s.queueStartupLocked(now, eligible, dayEnd) {
			dirty = true
		}
	}
	startupBusy := s.startupBusyLocked()
	for _, target := range eligible {
		p := s.doc.Plans[target.key]
		if p.Status == "success" || p.Status == "missed" || p.Status == "running" {
			continue
		}
		if p.StartupAt != 0 && startupBusy {
			continue
		}
		attempt, until := 0, retry
		if p.Status == "pending" && p.Attempts == 0 && p.StartupAt != 0 &&
			!now.Before(start) && now.Before(dayEnd) && s.startupDueLocked(p) <= now.UnixMilli() {
			attempt, until = 1, dayEnd
		} else if !startup && p.Status == "pending" && p.Attempts == 0 && p.StartupAt == 0 &&
			!now.Before(start) && now.Before(retry) && p.FirstAt <= now.UnixMilli() {
			attempt = 1
		} else if !startup && p.Status == "failed" && p.Attempts == 1 && !p.RetryEnd &&
			!now.Before(retry) && now.Before(end) && p.RetryAt <= now.UnixMilli() &&
			(p.StartupAt == 0 || s.startupDueLocked(p) <= now.UnixMilli()) {
			attempt, until = 2, end
		}
		if attempt == 0 {
			continue
		}
		p.Attempts, p.Status, p.LastAt = attempt, "running", now.UnixMilli()
		if p.StartupAt != 0 {
			s.doc.LastStartupAt = now.UnixMilli()
			startupBusy = true
		}
		p.Message = "正在按北京时间计划签到"
		trace := newTraceID()
		jobs = append(jobs, checkinJob{key: target.key, date: day, attempt: attempt, end: until, account: target.account, trace: trace, started: now})
		dirty = true
	}
	trace := newTraceID()
	if dirty && !s.saveLocked(trace) {
		return nil
	}
	for _, job := range jobs {
		log.Printf("[每日签到] traceId=%s 阶段=尝试预占已落盘 日期=%s 账号标识=%s 尝试=%d 截止=%s 结果=允许执行 说明=同日最多两次；启动补签间隔至少一分钟",
			job.trace, job.date, job.key[:12], job.attempt, job.end.In(checkinZone).Format("15:04:05"))
	}
	next := s.nextLocked(now)
	nextMillis := int64(0)
	if !next.IsZero() {
		nextMillis = next.UnixMilli()
	}
	if nextMillis != s.lastNext || startup || dirty {
		s.lastNext = nextMillis
		logScheduledTask("daily_checkin", "scheduled", "", trace, time.Time{}, next,
			"北京时间08:00–09:30随机；08:00后启动待签账号每隔一分钟补签；失败09:30–10:00仅重试一次；账号事件不触发补签")
	}
	return jobs
}

func (s *dailyCheckinScheduler) startupDueLocked(p *checkinRecord) int64 {
	return max(p.StartupAt, s.doc.LastStartupAt+time.Minute.Milliseconds())
}

func (s *dailyCheckinScheduler) startupBusyLocked() bool {
	for _, p := range s.doc.Plans {
		if p.StartupAt != 0 && p.Status == "running" {
			return true
		}
	}
	return false
}

func (s *dailyCheckinScheduler) queueStartupLocked(now time.Time, targets []checkinTarget, dayEnd time.Time) bool {
	var pending []checkinTarget
	for _, t := range targets {
		p := s.doc.Plans[t.key]
		if p.Status == "pending" && p.Attempts == 0 {
			pending = append(pending, t)
		}
	}
	// Keep already-persisted queue order across repeated restarts.
	sort.SliceStable(pending, func(i, j int) bool {
		a, b := s.doc.Plans[pending[i].key].StartupAt, s.doc.Plans[pending[j].key].StartupAt
		if a == 0 {
			return false
		}
		return b == 0 || a < b
	})
	slot := max(now.UnixMilli(), s.doc.LastStartupAt+time.Minute.Milliseconds())
	dirty := false
	for _, t := range pending {
		p := s.doc.Plans[t.key]
		at := max(slot, p.StartupAt)
		if at >= dayEnd.UnixMilli() {
			p.Status, p.Message = "missed", "启动补签队列已超过北京时间今天；不跨日补签"
			dirty = true
			continue
		}
		if p.StartupAt != at {
			p.StartupAt = at
			dirty = true
		}
		message := "启动补签排队中；前一请求完成后至少间隔一分钟"
		if p.Message != message {
			p.Message = message
			dirty = true
		}
		slot = at + time.Minute.Milliseconds()
	}
	if len(pending) > 0 {
		log.Printf("[签到计划] traceId=%s 阶段=启动补签排队 日期=%s 待签账号=%d 间隔秒=60 说明=仅尝试次数为零的待签账号；已签及失败账号不入队；计划先落盘",
			newTraceID(), s.doc.Date, len(pending))
	}
	return dirty
}

func (s *dailyCheckinScheduler) nextLocked(now time.Time) time.Time {
	var next time.Time
	start, _, retryEnd := checkinWindows(now)
	for key, p := range s.doc.Plans {
		if !s.active[key] {
			continue
		}
		at := int64(0)
		if p.Status == "pending" && p.Attempts == 0 {
			at = p.FirstAt
			if p.StartupAt != 0 {
				at = s.startupDueLocked(p)
				if at >= start.Add(16*time.Hour).UnixMilli() {
					continue
				}
			}
		} else if p.Status == "failed" && p.Attempts == 1 && !p.RetryEnd {
			at = p.RetryAt
			if p.StartupAt != 0 {
				at = max(at, s.startupDueLocked(p))
			}
			if at >= retryEnd.UnixMilli() {
				continue
			}
		}
		if at > now.UnixMilli() && (next.IsZero() || time.UnixMilli(at).Before(next)) {
			next = time.UnixMilli(at)
		}
	}
	return next
}

func nextDailyCheckin(now time.Time) time.Time {
	dailyCheckins.mu.Lock()
	defer dailyCheckins.mu.Unlock()
	if next := dailyCheckins.nextLocked(now); !next.IsZero() {
		return next
	}
	start, _, _ := checkinWindows(now)
	if now.Before(start) {
		return start
	}
	start, _, _ = checkinWindows(now.In(checkinZone).AddDate(0, 0, 1))
	return start
}

func (s *dailyCheckinScheduler) runJob(ctx context.Context, job checkinJob) {
	logScheduledTask("daily_checkin", "started", "running", job.trace, job.started, time.Time{},
		fmt.Sprintf("北京时间日期=%s 第%d次尝试；账号标识=%s", job.date, job.attempt, job.key[:12]))
	// Derive the remaining budget from the same clock used for the schedule.
	// This also keeps deterministic virtual-clock tests independent of wall time.
	ctx, cancel := context.WithTimeout(ctx, job.end.Sub(s.now()))
	defer cancel()
	ctx = context.WithValue(ctx, checkinExpectedIdentity{}, job.key)
	result, err := "failed", error(context.DeadlineExceeded)
	if s.now().Before(job.end) && ctx.Err() == nil {
		result, err = s.perform(ctx, job.account, job.trace)
	}
	now := s.now()
	s.mu.Lock()
	p := s.doc.Plans[job.key]
	if s.doc.Date != job.date || p == nil || p.Attempts != job.attempt || p.Status != "running" {
		s.mu.Unlock()
		return
	}
	status := "failed"
	if p.StartupAt != 0 && checkinDay(now) == job.date {
		// Persist completion too: slow credential reads/network IO must not
		// collapse the one-minute gap between actual requests.
		s.doc.LastStartupAt = max(s.doc.LastStartupAt, now.UnixMilli())
	}
	if err == nil && (result == "ok" || result == "already") {
		p.Status, p.Message, status = "success", "北京时间今天已签到", "success"
	} else {
		p.Status, p.Message = "failed", safeCheckinFailure(err)
		if job.attempt == 2 {
			p.RetryEnd = true
			p.Message += "；今日已用完两次尝试，不再重试"
		} else {
			_, _, retryEnd := checkinWindows(now)
			if !now.Before(retryEnd) {
				p.RetryEnd = true
				p.Message += "；已过今日失败重试窗口，不再重试"
			}
		}
	}
	s.saveLocked(job.trace)
	next := s.nextLocked(now)
	s.mu.Unlock()
	logScheduledTask("daily_checkin", "completed", status, job.trace, job.started, next,
		fmt.Sprintf("日期=%s 尝试=%d 账号标识=%s 结果=%s", job.date, job.attempt, job.key[:12], status))
	if status == "success" {
		requestQuotaScan()
	}
	if err := writeStatusSnapshot(); err != nil {
		log.Printf("[每日签到] traceId=%s 阶段=展示快照 结果=失败 说明=签到次数已保存，下次状态快照周期会重试展示，不重发签到", job.trace)
	}
}

func backgroundDailyCheckin(ctx context.Context) {
	dailyCheckins.run(ctx)
}

func (s *dailyCheckinScheduler) run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	var workers sync.WaitGroup
	defer workers.Wait()
	dispatch := func(jobs []checkinJob) {
		for _, job := range jobs {
			workers.Add(1)
			go func() {
				defer workers.Done()
				s.runJob(ctx, job)
			}()
		}
	}
	dispatch(s.planAndClaim(s.now(), currentCheckinTargets(), true))
	log.Printf("[每日签到] traceId=%s 阶段=启动 结果=按北京时间恢复计划 说明=08:00前仅等待随机计划；08:00后待签账号立即开始一分钟间隔补签；账号事件不触发补签", newTraceID())
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			dispatch(s.planAndClaim(s.now(), currentCheckinTargets(), false))
		}
	}
}

func (s *dailyCheckinScheduler) view(target checkinTarget, now time.Time) checkinStatusSnapshot {
	out := checkinStatusSnapshot{Date: checkinDay(now), Status: "pending"}
	if !target.supported {
		out.Status, out.Message = "unsupported", "国际站或企业账号不参与个人签到"
		return out
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	if s.blocked {
		out.Status, out.Message = "unknown", "签到状态文件不可用；为防止重复请求已暂停签到"
		return out
	}
	if s.doc.Date == out.Date {
		if p := s.doc.Plans[target.key]; p != nil {
			out.Status, out.Attempts, out.LastAt, out.Message = p.Status, p.Attempts, p.LastAt/1000, p.Message
			if p.Status == "pending" && p.Attempts == 0 {
				out.NextAt = p.FirstAt / 1000
				if p.StartupAt != 0 {
					out.NextAt = s.startupDueLocked(p) / 1000
					start, _, _ := checkinWindows(now)
					if out.NextAt >= start.Add(16*time.Hour).Unix() {
						out.Status, out.NextAt = "missed", 0
						out.Message = "今日剩余时间不足以保持一分钟间隔；不跨日补签"
					}
				}
			} else if p.Status == "failed" && p.Attempts == 1 && !p.RetryEnd {
				out.NextAt = p.RetryAt / 1000
				if p.StartupAt != 0 {
					out.NextAt = max(p.RetryAt, s.startupDueLocked(p)) / 1000
				}
				_, _, retryEnd := checkinWindows(now)
				if out.NextAt >= retryEnd.Unix() {
					out.NextAt = 0
				}
			}
			return out
		}
	}
	_, retry, _ := checkinWindows(now)
	if !now.Before(retry) {
		out.Status, out.Message = "missed", "今日窗口已过，未发起签到；不补签"
	}
	if !target.eligible {
		out.Message = "账号当前不可用，不发送签到"
	}
	return out
}

func checkinDisplay(view checkinStatusSnapshot, edition string, now time.Time) string {
	if profileForEdition(edition).Key != profileCN.Key || view.Status == "unsupported" {
		return "—"
	}
	if view.Date != checkinDay(now) {
		_, retry, _ := checkinWindows(now)
		if !now.Before(retry) {
			return "未签"
		}
		return "待签"
	}
	switch view.Status {
	case "success":
		return "已签"
	case "failed":
		return "失败"
	case "missed":
		return "未签"
	case "unknown":
		return "未知"
	default:
		return "待签"
	}
}

type checkinRequestError struct{ status, code int }

func (e checkinRequestError) Error() string {
	return fmt.Sprintf("签到失败（HTTP %d，业务码 %d）", e.status, e.code)
}

func safeCheckinFailure(err error) string {
	var known checkinRequestError
	if errors.As(err, &known) {
		return known.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "签到请求超时或已超过执行窗口"
	}
	if errors.Is(err, context.Canceled) {
		return "签到请求已取消"
	}
	return "签到请求失败；详见对应 TraceID 日志"
}

func checkinAccountWithTrace(ctx context.Context, acc *Account, trace string) (string, error) {
	log.Printf("[每日签到] traceId=%s 阶段=请求入口 说明=仅使用已保存的随机计划，不模拟活动完成", trace)
	if acc == nil {
		return "skipped", nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if !tryAccountLockWithContext(ctx, acc.lock.TryRLock) {
		return "failed", ctx.Err()
	}
	accountMu.Lock()
	target := checkinTargetLocked(acc)
	var auth StoredAuth
	prof := *acc.Profile()
	if acc.Auth != nil {
		auth = *acc.Auth
	}
	accountMu.Unlock()
	acc.lock.RUnlock() // network IO never holds the model credential lock
	if expected, ok := ctx.Value(checkinExpectedIdentity{}).(string); ok && expected != target.key {
		log.Printf("[每日签到] traceId=%s 阶段=凭据快照校验 结果=跳过 原因=计划后账号身份已改变 说明=不会把旧账号尝试次数用于新账号", trace)
		return "skipped", nil
	}
	if !target.eligible {
		result := "skipped"
		if prof.Key != profileCN.Key {
			result = "global_skipped"
		} else if auth.Account.EnterpriseID != "" {
			result = "enterprise_skipped"
		}
		log.Printf("[每日签到] traceId=%s 阶段=资格检查 结果=跳过 类型=%s 说明=未发送上游，不改变调度状态", trace, result)
		return result, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, prof.dailyCheckinURL(), strings.NewReader("{}"))
	if err != nil {
		return "failed", err
	}
	commonHeaders(req, &prof)
	req.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
	req.Header.Set("X-Client-Platform", "web")
	if auth.Account.UID != "" {
		req.Header.Set("X-User-Id", auth.Account.UID)
	}
	if auth.Auth.Domain != "" {
		req.Header.Set("X-Domain", auth.Auth.Domain)
	}
	client := *cfg.HttpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	log.Printf("[每日签到] traceId=%s 阶段=请求上游 方法=POST 账号标识=%s 说明=单次发送，不自动网络重试，不记录凭据或响应原文", trace, target.key[:12])
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[每日签到] traceId=%s 阶段=请求异常 类型=%s 结果=失败 说明=保留原账号状态，是否再试仅由当天计划决定", trace, debugErrorType(err))
		return "failed", err
	}
	defer resp.Body.Close()
	var envelope struct {
		Code *int   `json:"code"`
		Msg  string `json:"msg"`
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	parseErr := json.Unmarshal(raw, &envelope)
	code := -1
	if envelope.Code != nil {
		code = *envelope.Code
	}
	log.Printf("[每日签到] traceId=%s 阶段=响应校验 HTTP=%d 业务码=%d 说明=只接受明确成功或已签到，不将错误响应算作成功", trace, resp.StatusCode, code)
	if readErr == nil && len(raw) <= 1<<20 && parseErr == nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 && code == 0 {
			return "ok", nil
		}
		if (resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode == 400 || resp.StatusCode == 409) &&
			(code == 10001 || code == 14001 || code == -1 && isAlreadyCheckedIn(resp.StatusCode, envelope.Msg)) {
			return "already", nil
		}
	}
	return "failed", checkinRequestError{status: resp.StatusCode, code: code}
}
