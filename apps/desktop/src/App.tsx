import { useCallback, useEffect, useMemo, useRef, useState, type DragEvent, type ReactNode } from "react";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import {
  faArrowsRotate,
  faCalendarDays,
  faCircleStop,
  faClock,
  faCopy,
  faDownload,
  faEye,
  faFileImport,
  faFloppyDisk,
  faFolderOpen,
  faFolderPlus,
  faGear,
  faLink,
  faList,
  faMagnifyingGlass,
  faPause,
  faPlay,
  faPlug,
  faPlugCircleXmark,
  faPlus,
  faRotateRight,
  faStop,
  faTrash,
  faTrashCan,
  faXmark,
} from "@fortawesome/free-solid-svg-icons";
import { open } from "@tauri-apps/plugin-dialog";
import { revealItemInDir } from "@tauri-apps/plugin-opener";
import { call, onEvent } from "./api";
import { Details } from "./components/Details";
import { DownloadTable, type SortKey } from "./components/Table";
import { bytes, speed } from "./format";
import { categoryIcon, Icon, IconButton, libraryIcon, queueIcon } from "./icons";
import { useSteady } from "./steady";
import type { Category, Download, Filter, GrabbedLink, HarborEvent, ProbeResult, Queue, Schedule, Segment, Settings, Summary } from "./types";

const emptySummary: Summary = { all: 0, downloading: 0, queued: 0, completed: 0, paused: 0, failed: 0, speed_bps: 0, categories: [], queues: [] };

export function App() {
  const [downloads, setDownloads] = useState<Download[]>([]);
  const [summary, setSummary] = useState<Summary>(emptySummary);
  const [categories, setCategories] = useState<Category[]>([]);
  const [queues, setQueues] = useState<Queue[]>([]);
  const [settings, setSettings] = useState<Settings | null>(null);
  const [filter, setFilter] = useState<Filter>({ kind: "all" });
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [segments, setSegments] = useState<Segment[]>([]);
  const [sortKey, setSortKey] = useState<SortKey>("added");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [dialog, setDialog] = useState<null | "add" | "schedule" | "settings" | "grabber" | "batch" | "category" | "delete">(null);
  const [addUrl, setAddUrl] = useState("");
  const [toasts, setToasts] = useState<{ id: number; text: string }[]>([]);
  const [menu, setMenu] = useState<{ x: number; y: number } | null>(null);
  const [connected, setConnected] = useState(true);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const selectedRef = useRef<string[]>([]);
  selectedRef.current = selected;

  const toast = useCallback((text: string) => {
    const id = Date.now();
    setToasts((current) => [...current, { id, text }]);
    setTimeout(() => setToasts((current) => current.filter((item) => item.id !== id)), 4200);
  }, []);

  const refresh = useCallback(async () => {
    const params: Record<string, unknown> = { query };
    if (filter.kind === "category") params.category_id = filter.id;
    else if (filter.kind === "queue") params.queue_id = filter.id;
    else params.filter = filter.kind;
    const [rows, nextSummary, nextCategories, nextQueues] = await Promise.all([
      call<Download[]>("downloads.list", params),
      call<Summary>("downloads.summary"),
      call<Category[]>("categories.list"),
      call<Queue[]>("queues.list"),
    ]);
    setDownloads(rows);
    setSummary(nextSummary);
    setCategories(nextCategories);
    setQueues(nextQueues);
    setConnected(true);
  }, [filter, query]);

  useEffect(() => {
    refresh().catch((error: Error) => {
      setConnected(false);
      toast(error.message);
    });
    call<Settings>("settings.get").then(setSettings).catch(() => undefined);
    const timer = setInterval(() => { refresh().catch(() => setConnected(false)); }, 2000);
    let stopListening: (() => void) | undefined;
    let cancelled = false;
    onEvent((event) => applyEvent(event, selectedRef.current[0], setDownloads, setSegments, setAddUrl, setDialog, toast)).then((stop) => {
      if (cancelled) stop();
      else stopListening = stop;
    });
    return () => { cancelled = true; clearInterval(timer); stopListening?.(); };
  }, [refresh, toast]);

  useEffect(() => {
    const active = selected[0];
    if (!active) { setSegments([]); return; }
    call<{ download: Download; segments: Segment[] }>("downloads.get", { id: active })
      .then((detail) => setSegments(detail.segments))
      .catch(() => undefined);
  }, [selected, downloads]);

  useEffect(() => {
    const root = document.documentElement;
    const theme = settings?.theme ?? "system";
    const dark = theme === "dark" || (theme === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
    root.dataset.theme = dark ? "dark" : "light";
  }, [settings]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      if (target.matches("input, textarea, select")) return;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "n") { event.preventDefault(); setDialog("add"); }
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "f") { event.preventDefault(); document.getElementById("search")?.focus(); }
      if (event.key === "Escape") { setDialog(null); setMenu(null); }
      if (event.key === " " && selected.length) { event.preventDefault(); void run("downloads.pause", selected); }
      if (event.key === "Delete" && selected.length) setDialog("delete");
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selected]);

  const rows = useMemo(() => sortRows(downloads, sortKey, sortDir), [downloads, sortKey, sortDir]);
  const shownSpeed = useSteady(summary.speed_bps);
  const active = downloads.find((item) => item.id === selected[0]) ?? null;
  const chosen = downloads.filter((item) => selected.includes(item.id));

  async function run(method: string, ids = selected) {
    await Promise.all(ids.map((id) => call(method, { id })));
    await refresh();
  }

  async function pickFolder(current?: string) {
    const folder = await open({ directory: true, multiple: false, defaultPath: current });
    return typeof folder === "string" ? folder : null;
  }

  function onDrop(event: DragEvent) {
    event.preventDefault();
    const text = event.dataTransfer.getData("text/uri-list") || event.dataTransfer.getData("text/plain");
    const urls = text.split(/\s+/).map((item) => item.trim()).filter((item) => /^https?:\/\//.test(item) || item.startsWith("ftp://"));
    if (urls.length === 1) { setAddUrl(urls[0]); setDialog("add"); }
    else if (urls.length > 1) {
      call("batch.import", { text: urls.join("\n"), start: false }).then(() => { toast(`Added ${urls.length} URLs`); return refresh(); }).catch((error: Error) => toast(error.message));
    }
  }

  return (
    <div className="app" onDragOver={(event) => event.preventDefault()} onDrop={onDrop} onClick={() => setMenu(null)}>
      <aside className="sidebar">
        <div className="brand">
          <img className="mark" src="/favicon.png" alt="" />
          <div><strong>Harbor</strong><span>Free and open source</span></div>
        </div>
        <nav className="nav" aria-label="Library">
          <SidebarButton icon={libraryIcon("all")} current={filter.kind === "all"} count={summary.all} onClick={() => setFilter({ kind: "all" })}>All downloads</SidebarButton>
          <SidebarButton icon={libraryIcon("downloading")} current={filter.kind === "downloading"} count={summary.downloading} onClick={() => setFilter({ kind: "downloading" })}>Downloading</SidebarButton>
          <SidebarButton icon={libraryIcon("queued")} current={filter.kind === "queued"} count={summary.queued} onClick={() => setFilter({ kind: "queued" })}>Queued</SidebarButton>
          <SidebarButton icon={libraryIcon("completed")} current={filter.kind === "completed"} count={summary.completed} onClick={() => setFilter({ kind: "completed" })}>Completed</SidebarButton>
          <SidebarButton icon={libraryIcon("paused")} current={filter.kind === "paused"} count={summary.paused} onClick={() => setFilter({ kind: "paused" })}>Paused</SidebarButton>
          <SidebarButton icon={libraryIcon("failed")} current={filter.kind === "failed"} count={summary.failed} onClick={() => setFilter({ kind: "failed" })}>Failed</SidebarButton>
          <div className="label">Categories</div>
          {categories.map((category) => (
            <SidebarButton key={category.id} icon={categoryIcon(category.name)} current={filter.kind === "category" && filter.id === category.id} count={summary.categories.find((item) => item.id === category.id)?.count ?? 0} onClick={() => setFilter({ kind: "category", id: category.id })}>
              {category.name}
            </SidebarButton>
          ))}
          <div className="label">Queues</div>
          {queues.map((queue) => (
            <SidebarButton key={queue.id} icon={queueIcon(queue.name)} current={filter.kind === "queue" && filter.id === queue.id} count={undefined} onClick={() => setFilter({ kind: "queue", id: queue.id })}>
              {queue.name}
            </SidebarButton>
          ))}
        </nav>
        <div className="side-actions">
          <IconButton icon={faFolderPlus} onClick={() => setDialog("category")}>New category</IconButton>
          <IconButton icon={faLink} onClick={() => setDialog("grabber")}>Grab links</IconButton>
        </div>
      </aside>
      <main className="main">
        <div className="toolbar" role="toolbar" aria-label="Download actions">
          <div className="tool-group">
            <IconButton className="primary" icon={faPlus} onClick={() => setDialog("add")}>Add URL</IconButton>
          </div>
          <span className="tool-sep" aria-hidden="true" />
          <div className="tool-group">
            <IconButton icon={faPlay} disabled={!chosen.some((item) => item.status !== "downloading" && item.status !== "completed")} onClick={() => void run("downloads.start")}>Start</IconButton>
            <IconButton icon={faRotateRight} disabled={!chosen.some((item) => item.status === "paused" || item.status === "stopped")} onClick={() => void run("downloads.resume")}>Resume</IconButton>
            <IconButton icon={faPause} disabled={!chosen.some((item) => item.status === "downloading")} onClick={() => void run("downloads.pause")}>Pause</IconButton>
            <IconButton icon={faStop} disabled={!chosen.some((item) => item.status === "downloading" || item.status === "queued")} onClick={() => void run("downloads.stop")}>Stop</IconButton>
            <IconButton icon={faCircleStop} onClick={() => void call("downloads.stop_all").then(refresh)}>Stop all</IconButton>
          </div>
          <span className="tool-sep" aria-hidden="true" />
          <div className="tool-group">
            <IconButton icon={faTrash} disabled={!selected.length} onClick={() => setDialog("delete")}>Delete</IconButton>
            <IconButton icon={faTrashCan} onClick={() => void call("downloads.delete_completed").then(refresh)}>Delete completed</IconButton>
          </div>
          <span className="tool-sep" aria-hidden="true" />
          <div className="tool-group">
            <IconButton icon={faCalendarDays} onClick={() => setDialog("schedule")}>Scheduler</IconButton>
            <IconButton icon={faFileImport} onClick={() => setDialog("batch")}>Import</IconButton>
            <IconButton icon={faGear} onClick={() => setDialog("settings")}>Settings</IconButton>
          </div>
          <label className="search">
            <Icon icon={faMagnifyingGlass} />
            <input id="search" aria-label="Search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filename or URL" />
          </label>
        </div>
        <DownloadTable
          rows={rows}
          selected={selected}
          sortKey={sortKey}
          sortDir={sortDir}
          onSort={(key) => { setSortDir(sortKey === key && sortDir === "asc" ? "desc" : "asc"); setSortKey(key); }}
          onSelect={(id, event) => setSelected(event.metaKey || event.ctrlKey ? (selected.includes(id) ? selected.filter((item) => item !== id) : [...selected, id]) : [id])}
          onActivate={(id) => { const item = downloads.find((row) => row.id === id); if (item) void revealItemInDir(`${item.destination_dir}/${item.filename}`); }}
          onContext={(id, x, y) => { if (!selected.includes(id)) setSelected([id]); setMenu({ x, y }); }}
        />
        <Details download={active} segments={segments} />
        <footer className="status-bar">
          <span className={connected ? "agent ok" : "agent bad"}>
            <Icon icon={connected ? faPlug : faPlugCircleXmark} />
            {connected ? "Agent connected" : "Agent unavailable"} · {summary.downloading} active · {speed(shownSpeed)}
          </span>
          <span><Icon icon={faList} />{summary.all} downloads</span>
        </footer>
      </main>
      {menu && (
        <div className="menu" style={{ left: menu.x, top: menu.y }} role="menu">
          <IconButton icon={faPlay} onClick={() => void run("downloads.start")}>Start</IconButton>
          <IconButton icon={faPause} onClick={() => void run("downloads.pause")}>Pause</IconButton>
          <IconButton icon={faRotateRight} onClick={() => void run("downloads.resume")}>Resume</IconButton>
          <IconButton icon={faStop} onClick={() => void run("downloads.stop")}>Stop</IconButton>
          <IconButton icon={faCopy} onClick={() => { if (active) void navigator.clipboard.writeText(active.url); }}>Copy URL</IconButton>
          <IconButton icon={faFolderOpen} onClick={() => { if (active) void revealItemInDir(active.destination_dir); }}>Show in folder</IconButton>
          <IconButton icon={faTrash} onClick={() => setDialog("delete")}>Delete</IconButton>
        </div>
      )}
      <div className="toasts" aria-live="polite">{toasts.map((item) => <div className="toast" key={item.id}>{item.text}</div>)}</div>
      {dialog === "add" && <AddDialog initialUrl={addUrl} categories={categories} queues={queues} settings={settings} onClose={() => setDialog(null)} onPickFolder={pickFolder} onDone={async (message) => { setDialog(null); toast(message); await refresh(); }} />}
      {dialog === "settings" && settings && <SettingsDialog settings={settings} onClose={() => setDialog(null)} onPickFolder={pickFolder} onSave={async (next) => { const saved = await call<Settings>("settings.set", next as unknown as Record<string, unknown>); setSettings(saved); setDialog(null); toast("Settings saved"); }} />}
      {dialog === "schedule" && <SchedulerDialog queues={queues} onClose={() => setDialog(null)} onChanged={refresh} toast={toast} />}
      {dialog === "grabber" && <GrabberDialog categories={categories} onClose={() => setDialog(null)} onDone={async (count) => { setDialog(null); toast(`Added ${count} links`); await refresh(); }} />}
      {dialog === "batch" && <BatchDialog onClose={() => setDialog(null)} onDone={async (count) => { setDialog(null); toast(`Imported ${count} lines`); await refresh(); }} />}
      {dialog === "category" && <CategoryDialog onClose={() => setDialog(null)} onPickFolder={pickFolder} onSave={async (category) => { await call("categories.save", category); setDialog(null); await refresh(); }} />}
      {dialog === "delete" && (
        <Dialog title="Delete downloads" icon={faTrash} onClose={() => setDialog(null)}>
          <p>Remove {selected.length} download{selected.length === 1 ? "" : "s"} from Harbor.</p>
          <label className="check"><input type="checkbox" checked={deleteFiles} onChange={(event) => setDeleteFiles(event.target.checked)} /> Also delete files from disk</label>
          <div className="actions">
            <IconButton icon={faXmark} onClick={() => setDialog(null)}>Cancel</IconButton>
            <IconButton icon={faTrash} className="primary" onClick={() => { void Promise.all(selected.map((id) => call("downloads.delete", { id, delete_files: deleteFiles }))).then(refresh); setDialog(null); setSelected([]); }}>Delete</IconButton>
          </div>
        </Dialog>
      )}
    </div>
  );
}

function SidebarButton({ current, count, icon, onClick, children }: { current: boolean; count?: number; icon: IconDefinition; onClick: () => void; children: ReactNode }) {
  return (
    <button type="button" aria-current={current ? "page" : undefined} onClick={onClick}>
      <span className="nav-label"><Icon icon={icon} /><span>{children}</span></span>
      {count != null && <span className="count">{count}</span>}
    </button>
  );
}

function Dialog({ title, icon, onClose, children }: { title: string; icon: IconDefinition; onClose: () => void; children: ReactNode }) {
  return (
    <div className="backdrop" onMouseDown={onClose}>
      <div className="dialog" role="dialog" aria-modal="true" aria-label={title} onMouseDown={(event) => event.stopPropagation()}>
        <div className="dialog-head">
          <h2><Icon icon={icon} />{title}</h2>
          <button type="button" className="icon-button" aria-label="Close" onClick={onClose}><Icon icon={faXmark} /></button>
        </div>
        {children}
      </div>
    </div>
  );
}

function AddDialog({ initialUrl, categories, queues, settings, onClose, onPickFolder, onDone }: {
  initialUrl: string; categories: Category[]; queues: Queue[]; settings: Settings | null;
  onClose: () => void; onPickFolder: (current?: string) => Promise<string | null>; onDone: (message: string) => Promise<void>;
}) {
  const [url, setUrl] = useState(initialUrl);
  const [filename, setFilename] = useState("");
  const [folder, setFolder] = useState(settings?.default_directory ?? "");
  const [category, setCategory] = useState("");
  const [queue, setQueue] = useState("queue_main");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [headers, setHeaders] = useState("");
  const [cookies, setCookies] = useState("");
  const [info, setInfo] = useState<ProbeResult | null>(null);
  const [error, setError] = useState("");
  useEffect(() => { setUrl(initialUrl); }, [initialUrl]);
  async function probe() {
    setError("");
    try {
      const result = await call<ProbeResult>("downloads.probe", { url, username: username || null, password: password || null, cookies: cookies || null, headers: headerMap(headers) });
      setInfo(result);
      if (!filename) setFilename(result.filename);
      if (!category && result.category_id) setCategory(result.category_id);
    } catch (probeError) {
      setError(probeError instanceof Error ? probeError.message : "Could not read that URL");
    }
  }
  async function submit(start: boolean) {
    await call("downloads.add", {
      url, filename: filename || null, destination_dir: folder || null, category_id: category || null, queue_id: queue,
      username: username || null, password: password || null, cookies: cookies || null, headers: headerMap(headers), start,
    });
    await onDone(start ? "Download started" : "Saved for later");
  }
  return (
    <Dialog title="Add URL" icon={faLink} onClose={onClose}>
      <label>Download URL
        <span className="input-icon">
          <Icon icon={faLink} />
          <input value={url} onChange={(event) => setUrl(event.target.value)} onBlur={() => { if (url.startsWith("http") || url.startsWith("ftp")) void probe(); }} autoFocus />
        </span>
      </label>
      <div className="row">
        <label>Filename<input value={filename} onChange={(event) => setFilename(event.target.value)} placeholder={info?.filename ?? "Detected from the server"} /></label>
        <label>Category
          <select value={category} onChange={(event) => setCategory(event.target.value)}>
            <option value="">Automatic</option>
            {categories.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
          </select>
        </label>
      </div>
      <label>Save location
        <div className="row">
          <input value={folder} onChange={(event) => setFolder(event.target.value)} />
          <IconButton icon={faFolderOpen} onClick={() => void onPickFolder(folder).then((path) => { if (path) setFolder(path); })}>Browse</IconButton>
        </div>
      </label>
      <label>Queue
        <select value={queue} onChange={(event) => setQueue(event.target.value)}>
          {queues.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
        </select>
      </label>
      <div className="row">
        <label>Username<input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="off" /></label>
        <label>Password<input type="password" value={password} onChange={(event) => setPassword(event.target.value)} autoComplete="off" /></label>
      </div>
      <label>Custom headers<textarea rows={3} value={headers} onChange={(event) => setHeaders(event.target.value)} placeholder={"Referer: https://example.com"} /></label>
      <label>Cookies<textarea rows={2} value={cookies} onChange={(event) => setCookies(event.target.value)} placeholder="session=..." /></label>
      {info && <p>{info.total_bytes ? bytes(info.total_bytes) : "Size unknown"} · HTTP {info.http_status} · {info.accelerated_possible ? "Segmented download available" : "Single connection"}</p>}
      {error && <p className="error">{error}</p>}
      <div className="actions">
        <IconButton icon={faXmark} onClick={onClose}>Cancel</IconButton>
        <IconButton icon={faMagnifyingGlass} onClick={() => void probe()}>Check URL</IconButton>
        <IconButton icon={faClock} onClick={() => void submit(false)}>Download later</IconButton>
        <IconButton icon={faDownload} className="primary" onClick={() => void submit(true)}>Download now</IconButton>
      </div>
    </Dialog>
  );
}

function SettingsDialog({ settings, onClose, onSave, onPickFolder }: { settings: Settings; onClose: () => void; onSave: (settings: Settings) => Promise<void>; onPickFolder: (current?: string) => Promise<string | null> }) {
  const [draft, setDraft] = useState(settings);
  const set = (patch: Partial<Settings>) => setDraft({ ...draft, ...patch });
  return (
    <Dialog title="Settings" icon={faGear} onClose={onClose}>
      <p>Harbor is free under the MIT license. There is no paid unlock. Leave bandwidth and the hourly quota at 0 for full speed.</p>
      <label>Theme
        <select value={draft.theme} onChange={(event) => set({ theme: event.target.value })}>
          <option value="system">System</option><option value="light">Light</option><option value="dark">Dark</option>
        </select>
      </label>
      <label>Default folder
        <div className="row"><input value={draft.default_directory} onChange={(event) => set({ default_directory: event.target.value })} /><IconButton icon={faFolderOpen} onClick={() => void onPickFolder(draft.default_directory).then((path) => { if (path) set({ default_directory: path }); })}>Browse</IconButton></div>
      </label>
      <div className="row">
        <label>Max connections<input type="number" min={1} max={32} value={draft.max_connections} onChange={(event) => set({ max_connections: Number(event.target.value) })} /></label>
        <label>Bandwidth KB/s (0 unlimited)<input type="number" min={0} value={Math.round(draft.bandwidth_limit_bps / 1024)} onChange={(event) => set({ bandwidth_limit_bps: Number(event.target.value) * 1024 })} /></label>
      </div>
      <div className="row">
        <label>Hourly quota MB (0 unlimited)<input type="number" min={0} value={Math.round(draft.quota_bytes_per_hour / (1024 * 1024))} onChange={(event) => set({ quota_bytes_per_hour: Number(event.target.value) * 1024 * 1024 })} /></label>
        <label>Proxy URL<input value={draft.proxy_url} onChange={(event) => set({ proxy_url: event.target.value })} placeholder="http://127.0.0.1:8080" /></label>
      </div>
      <label>Antivirus command<input value={draft.antivirus_command} onChange={(event) => set({ antivirus_command: event.target.value })} placeholder="/usr/bin/clamscan" /></label>
      <label>Update manifest URL<input value={draft.update_manifest_url} onChange={(event) => set({ update_manifest_url: event.target.value })} /></label>
      <label className="check"><input type="checkbox" checked={draft.clipboard_watch} onChange={(event) => set({ clipboard_watch: event.target.checked })} /> Watch the clipboard for URLs</label>
      <label className="check"><input type="checkbox" checked={draft.intercept_downloads} onChange={(event) => set({ intercept_downloads: event.target.checked })} /> Browser extension may intercept downloads</label>
      <label className="check"><input type="checkbox" checked={draft.notifications} onChange={(event) => set({ notifications: event.target.checked })} /> Notifications</label>
      <div className="actions">
        <IconButton icon={faArrowsRotate} onClick={() => void call("updates.check").then((result) => alert(JSON.stringify(result)))}>Check for updates</IconButton>
        <IconButton icon={faXmark} onClick={onClose}>Cancel</IconButton>
        <IconButton icon={faFloppyDisk} className="primary" onClick={() => void onSave(draft)}>Save</IconButton>
      </div>
    </Dialog>
  );
}

function SchedulerDialog({ queues, onClose, onChanged, toast }: { queues: Queue[]; onClose: () => void; onChanged: () => Promise<void>; toast: (text: string) => void }) {
  const [queueId, setQueueId] = useState(queues[0]?.id ?? "queue_main");
  const [name, setName] = useState("");
  const [concurrent, setConcurrent] = useState(3);
  const [start, setStart] = useState("");
  const [stop, setStop] = useState("");
  const [recurrence, setRecurrence] = useState("once");
  const queue = queues.find((item) => item.id === queueId);
  useEffect(() => {
    if (!queue) return;
    setName(queue.name);
    setConcurrent(queue.max_concurrent);
    call<Schedule[]>("schedules.list").then((rows) => {
      const schedule = rows.find((item) => item.queue_id === queue.id);
      if (!schedule) return;
      setRecurrence(schedule.recurrence);
      if (schedule.start_at) setStart(toLocal(schedule.start_at));
      if (schedule.stop_minutes != null) setStop(minutesToClock(schedule.stop_minutes));
    }).catch(() => undefined);
  }, [queueId, queue]);
  return (
    <Dialog title="Scheduler" icon={faCalendarDays} onClose={onClose}>
      <label>Queue
        <select value={queueId} onChange={(event) => setQueueId(event.target.value)}>
          {queues.map((item) => <option key={item.id} value={item.id}>{item.name} · {item.status}</option>)}
        </select>
      </label>
      <div className="row">
        <label>Name<input value={name} onChange={(event) => setName(event.target.value)} /></label>
        <label>Concurrent<input type="number" min={1} max={10} value={concurrent} onChange={(event) => setConcurrent(Number(event.target.value))} /></label>
      </div>
      <div className="row">
        <label>Start<input type="datetime-local" value={start} onChange={(event) => setStart(event.target.value)} /></label>
        <label>Stop<input type="time" value={stop} onChange={(event) => setStop(event.target.value)} /></label>
      </div>
      <label>Repeat
        <select value={recurrence} onChange={(event) => setRecurrence(event.target.value)}>
          <option value="once">Once</option><option value="daily">Daily</option><option value="weekly">Weekly</option>
        </select>
      </label>
      <div className="actions">
        <IconButton icon={faPlus} onClick={() => void call("queues.save", { name: "New queue", kind: "download", max_concurrent: 2 }).then(onChanged).then(() => toast("Queue created"))}>New queue</IconButton>
        <IconButton icon={faPlay} onClick={() => void call("queues.start", { id: queueId }).then(onChanged)}>Start now</IconButton>
        <IconButton icon={faPause} onClick={() => void call("queues.pause", { id: queueId }).then(onChanged)}>Pause</IconButton>
        <IconButton icon={faStop} onClick={() => void call("queues.stop", { id: queueId }).then(onChanged)}>Stop</IconButton>
        <IconButton icon={faFloppyDisk} className="primary" onClick={() => void saveSchedule()}>Save</IconButton>
      </div>
    </Dialog>
  );
  async function saveSchedule() {
    await call("queues.save", { id: queueId, name, kind: queue?.kind ?? "download", max_concurrent: concurrent });
    const [hours, minutes] = stop.split(":").map(Number);
    await call("schedules.save", {
      queue_id: queueId,
      start_at: start ? new Date(start).getTime() : null,
      stop_minutes: stop ? hours * 60 + minutes : null,
      recurrence,
      enabled: Boolean(start || stop),
    });
    await onChanged();
    toast("Schedule saved");
    onClose();
  }
}

function GrabberDialog({ onClose, onDone }: { categories: Category[]; onClose: () => void; onDone: (count: number) => Promise<void> }) {
  const [url, setUrl] = useState("");
  const [extensions, setExtensions] = useState("zip pdf mp4 mp3");
  const [domain, setDomain] = useState("");
  const [links, setLinks] = useState<GrabbedLink[]>([]);
  const [picked, setPicked] = useState<string[]>([]);
  return (
    <Dialog title="Grab links" icon={faLink} onClose={onClose}>
      <label>Page URL<input value={url} onChange={(event) => setUrl(event.target.value)} /></label>
      <div className="row">
        <label>Extensions<input value={extensions} onChange={(event) => setExtensions(event.target.value)} /></label>
        <label>Domain<input value={domain} onChange={(event) => setDomain(event.target.value)} placeholder="optional" /></label>
      </div>
      <div className="links">
        {links.map((link) => (
          <label className="check" key={link.url}>
            <input type="checkbox" checked={picked.includes(link.url)} onChange={(event) => setPicked(event.target.checked ? [...picked, link.url] : picked.filter((item) => item !== link.url))} />
            {link.filename}
          </label>
        ))}
      </div>
      <div className="actions">
        <IconButton icon={faEye} onClick={() => void call<GrabbedLink[]>("grabber.preview", { url, extensions, domain, depth: 2 }).then((found) => { setLinks(found); setPicked(found.map((link) => link.url)); })}>Preview</IconButton>
        <IconButton icon={faPlus} className="primary" onClick={() => void call("batch.import", { text: picked.join("\n"), start: false }).then(() => onDone(picked.length))}>Add selected</IconButton>
      </div>
    </Dialog>
  );
}

function BatchDialog({ onClose, onDone }: { onClose: () => void; onDone: (count: number) => Promise<void> }) {
  const [text, setText] = useState("");
  return (
    <Dialog title="Import URLs" icon={faFileImport} onClose={onClose}>
      <textarea rows={8} value={text} onChange={(event) => setText(event.target.value)} placeholder="One URL per line" />
      <div className="actions">
        <IconButton icon={faXmark} onClick={onClose}>Cancel</IconButton>
        <IconButton icon={faFileImport} className="primary" onClick={() => void call("batch.import", { text, start: false }).then(() => onDone(text.split("\n").filter((line) => line.trim()).length))}>Import</IconButton>
      </div>
    </Dialog>
  );
}

function CategoryDialog({ onClose, onSave, onPickFolder }: { onClose: () => void; onSave: (category: Record<string, unknown>) => Promise<void>; onPickFolder: (current?: string) => Promise<string | null> }) {
  const [name, setName] = useState("");
  const [extensions, setExtensions] = useState("");
  const [directory, setDirectory] = useState("");
  const [sites, setSites] = useState("");
  return (
    <Dialog title="New category" icon={faFolderPlus} onClose={onClose}>
      <label>Name<input value={name} onChange={(event) => setName(event.target.value)} /></label>
      <label>Extensions<input value={extensions} onChange={(event) => setExtensions(event.target.value)} placeholder="epub mobi" /></label>
      <label>Folder<div className="row"><input value={directory} onChange={(event) => setDirectory(event.target.value)} /><IconButton icon={faFolderOpen} onClick={() => void onPickFolder().then((path) => { if (path) setDirectory(path); })}>Browse</IconButton></div></label>
      <label>Only from these sites<input value={sites} onChange={(event) => setSites(event.target.value)} placeholder="cdn.example.com" /></label>
      <div className="actions"><IconButton icon={faXmark} onClick={onClose}>Cancel</IconButton><IconButton icon={faFloppyDisk} className="primary" onClick={() => void onSave({ name, extensions, directory, site_filter: sites })}>Save</IconButton></div>
    </Dialog>
  );
}

function applyEvent(event: HarborEvent, selectedId: string | undefined, setDownloads: (value: Download[] | ((current: Download[]) => Download[])) => void, setSegments: (segments: Segment[]) => void, setAddUrl: (url: string) => void, setDialog: (dialog: "add" | null) => void, toast: (text: string) => void) {
  if (event.event === "progress" && event.download_id) {
    setDownloads((current) => current.map((item) => item.id === event.download_id ? {
      ...item,
      downloaded_bytes: event.downloaded_bytes ?? item.downloaded_bytes,
      total_bytes: event.total_bytes === undefined ? item.total_bytes : event.total_bytes,
      speed_bps: event.speed_bps ?? item.speed_bps,
      average_bps: event.average_bps ?? item.average_bps,
      eta_secs: event.eta_secs === undefined ? item.eta_secs : event.eta_secs,
      active_connections: event.active_connections ?? item.active_connections,
      accelerated: event.accelerated ?? item.accelerated,
    } : item));
    if (event.segments && event.download_id === selectedId) setSegments(event.segments);
  }
  if (event.event === "status" && event.download_id) {
    setDownloads((current) => current.map((item) => item.id === event.download_id ? { ...item, status: event.status ?? item.status, error: event.error ?? item.error, warning: event.warning ?? item.warning } : item));
  }
  if (event.event === "clipboard" && event.url) { setAddUrl(event.url); setDialog("add"); }
  if (event.event === "notification" && event.title) toast(`${event.title}. ${event.body ?? ""}`);
}

function sortRows(rows: Download[], key: SortKey, dir: "asc" | "desc") {
  const factor = dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const left = sortValue(a, key);
    const right = sortValue(b, key);
    if (left < right) return -1 * factor;
    if (left > right) return 1 * factor;
    return 0;
  });
}

function sortValue(row: Download, key: SortKey): string | number {
  switch (key) {
    case "filename": return row.filename.toLowerCase();
    case "type": return row.filename.split(".").pop() ?? "";
    case "size": return row.total_bytes ?? -1;
    case "progress": return row.total_bytes ? row.downloaded_bytes / row.total_bytes : 0;
    case "speed": return row.speed_bps;
    case "eta": return row.eta_secs ?? Number.MAX_SAFE_INTEGER;
    case "status": return row.status;
    case "category": return row.category_name ?? "";
    case "added": return row.created_at;
  }
}

function headerMap(text: string) {
  const headers: Record<string, string> = {};
  for (const line of text.split("\n")) {
    const [key, ...rest] = line.split(":");
    if (key && rest.length) headers[key.trim()] = rest.join(":").trim();
  }
  return headers;
}

function toLocal(millis: number) {
  const date = new Date(millis);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function minutesToClock(minutes: number) {
  return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
}
