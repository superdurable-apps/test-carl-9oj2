import { useEffect, useRef, useState } from 'react';
import { getRadar, requestRadarRun } from './api/generated/sdk.gen';
import type { RadarHotspot, RadarSnapshot } from './api/generated/types.gen';

type Filter = 'all' | 'today' | 'confirmed' | 'special';
const pollMs = 2000;

function dayLabel(value: string, now: Date): string {
  const date = new Date(value);
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const previous = new Date(today);
  previous.setDate(previous.getDate() - 1);
  if (date >= today) return '今天';
  if (date >= previous) return '昨天';
  return '更早';
}

function isSpecial(hotspot: RadarHotspot): boolean {
  return ['science', 'medical', 'neuroscience'].includes(hotspot.domain);
}

export function App() {
  const [snapshot, setSnapshot] = useState<RadarSnapshot | null>(null);
  const [filter, setFilter] = useState<Filter>('all');
  const [running, setRunning] = useState(false);
  const [error, setError] = useState('');
  const controller = useRef<AbortController | null>(null);

  useEffect(() => {
    const current = new AbortController();
    controller.current = current;
    getRadar({ signal: current.signal }).then((response) => {
      if (!current.signal.aborted) {
        if (response.data) setSnapshot(response.data);
        else setError(response.error?.message ?? '无法读取看板');
      }
    }).catch((failure: unknown) => {
      if (!current.signal.aborted) setError(failure instanceof Error ? failure.message : '无法读取看板');
    });
    return () => current.abort();
  }, []);

  async function runRound() {
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    setRunning(true);
    setError('');
    try {
      const before = snapshot?.roundNumber ?? 0;
      const requestId = crypto.randomUUID();
      const started = await requestRadarRun({ body: { requestId }, signal: current.signal });
      if (!started.data) throw new Error(started.error?.message ?? '未能启动采集');
      const deadline = Date.now() + 5 * 60_000;
      while (!current.signal.aborted && Date.now() < deadline) {
        const response = await getRadar({ signal: current.signal });
        if (!response.data) throw new Error(response.error?.message ?? '无法读取结果');
        setSnapshot(response.data);
        if (response.data.roundNumber > before && response.data.status === 'ready') break;
        await new Promise((resolve) => setTimeout(resolve, pollMs));
      }
    } catch (failure) {
      if (!current.signal.aborted) setError(failure instanceof Error ? failure.message : '采集失败');
    } finally {
      if (controller.current === current) setRunning(false);
    }
  }

  function download() {
    if (!snapshot?.html) return;
    const url = URL.createObjectURL(new Blob([snapshot.html], { type: 'text/html;charset=utf-8' }));
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'ai-hotspot-radar.html';
    anchor.click();
    URL.revokeObjectURL(url);
  }

  const now = new Date();
  const visible = (snapshot?.hotspots ?? []).filter((hotspot) => {
    if (filter === 'today') return dayLabel(hotspot.firstSeen, now) === '今天';
    if (filter === 'special') return isSpecial(hotspot);
    return true;
  });
  const groups = ['今天', '昨天', '更早'] as const;

  return (
    <main>
      <header className="hero">
        <p className="eyebrow">AI HOTSPOT RADAR</p>
        <h1>热点发现</h1>
        <p>从 Hacker News 发现新词，结合历史基线与 AI 判读。</p>
        <div className="overview">
          <div><strong>{snapshot?.newHotspots ?? 0}</strong><span>今日新增</span></div>
          <div><strong>{snapshot?.hotspots.length ?? 0}</strong><span>已确认热点</span></div>
          <div><strong>{snapshot?.pendingCount ?? 0}</strong><span>待判词</span></div>
        </div>
        <div className="actions">
          <button type="button" onClick={runRound} disabled={running}>{running ? '采集中…' : '立即采集'}</button>
          <button type="button" className="secondary" onClick={download} disabled={!snapshot?.html}>下载静态 HTML</button>
        </div>
        {snapshot?.lastRunAt && <p className="muted">最近运行：{new Date(snapshot.lastRunAt).toLocaleString()}</p>}
        {snapshot?.report && <pre aria-label="最近一轮汇报">{snapshot.report}</pre>}
        {error && <p role="alert">{error}</p>}
      </header>
      <nav className="filters" aria-label="热点筛选">
        {([
          ['all', '全部'], ['today', '今天'], ['confirmed', '已确认'], ['special', '科学医疗神经'],
        ] as const).map(([value, label]) => (
          <button type="button" key={value} aria-pressed={filter === value} onClick={() => setFilter(value)}>{label}</button>
        ))}
      </nav>
      {!snapshot?.lastRunAt && <p className="empty">还没有采集结果。点击“立即采集”启动第一轮。</p>}
      {snapshot?.lastRunAt && visible.length === 0 && <p className="empty">当前筛选没有热点。</p>}
      {groups.map((group) => {
        const entries = visible.filter((hotspot) => dayLabel(hotspot.firstSeen, now) === group);
        if (entries.length === 0) return null;
        return (
          <section key={group} aria-label={group}>
            <h2>{group}<span>{entries.length}</span></h2>
            <div className="cards">
              {entries.map((hotspot) => (
                <article className="card" key={hotspot.name}>
                  <div className="card-top"><h3>{hotspot.name}</h3><span>{hotspot.domain} · {hotspot.authorCount} 位作者</span></div>
                  <p>{hotspot.concept}</p>
                  <p className="tags">{hotspot.primitives.join(' · ')}</p>
                  <details>
                    <summary>原语与证据</summary>
                    <p>English primitives: {hotspot.primitives_en.join(', ')}</p>
                    <p>搜索词：{hotspot.search_terms.join(' · ')}</p>
                    <ul>
                      {hotspot.evidence.map((item) => (
                        <li key={item.id}><a href={item.url} target="_blank" rel="noopener noreferrer">{item.title}</a><span>{item.author} · {item.score} 分</span></li>
                      ))}
                    </ul>
                  </details>
                </article>
              ))}
            </div>
          </section>
        );
      })}
    </main>
  );
}
