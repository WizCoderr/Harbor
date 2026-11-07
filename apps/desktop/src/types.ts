export type DownloadStatus =
  | "queued"
  | "downloading"
  | "paused"
  | "completed"
  | "failed"
  | "stopped"
  | "scheduled";

export interface Download {
  id: string;
  url: string;
  final_url: string | null;
  filename: string;
  destination_dir: string;
  category_id: string | null;
  category_name: string | null;
  queue_id: string | null;
  queue_name: string | null;
  status: DownloadStatus;
  total_bytes: number | null;
  downloaded_bytes: number;
  mime: string | null;
  http_status: number | null;
  resume_capable: boolean;
  accelerated: boolean;
  error: string | null;
  warning: string | null;
  has_credentials: boolean;
  speed_bps: number;
  average_bps: number;
  eta_secs: number | null;
  active_connections: number;
  created_at: number;
  sha256: string | null;
  etag: string | null;
}

export interface Segment {
  id: string;
  start_byte: number;
  end_byte: number;
  received_bytes: number;
  status: string;
}

export interface Category {
  id: string;
  name: string;
  extensions: string[];
  directory: string | null;
  site_filter: string[];
  builtin: boolean;
}

export interface Queue {
  id: string;
  name: string;
  kind: "download" | "sync";
  max_concurrent: number;
  status: "idle" | "running" | "paused";
}

export interface Schedule {
  id: string;
  queue_id: string;
  start_at: number | null;
  stop_minutes: number | null;
  recurrence: "once" | "daily" | "weekly";
  enabled: boolean;
}

export interface Summary {
  all: number;
  downloading: number;
  queued: number;
  completed: number;
  paused: number;
  failed: number;
  speed_bps: number;
  categories: { id: string; name: string; count: number }[];
  queues: Queue[];
}

export interface Settings {
  theme: string;
  max_connections: number;
  bandwidth_limit_bps: number;
  quota_bytes_per_hour: number;
  proxy_url: string;
  clipboard_watch: boolean;
  intercept_downloads: boolean;
  antivirus_command: string;
  default_directory: string;
  notifications: boolean;
  update_manifest_url: string;
}

export interface ProbeResult {
  final_url: string;
  filename: string;
  total_bytes: number | null;
  mime: string | null;
  accept_ranges: boolean;
  http_status: number;
  resume_capable: boolean;
  accelerated_possible: boolean;
  category_id: string | null;
}

export interface GrabbedLink {
  url: string;
  filename: string;
  extension: string;
}

export type Filter =
  | { kind: "all" | "downloading" | "queued" | "completed" | "paused" | "failed" }
  | { kind: "category"; id: string }
  | { kind: "queue"; id: string };

export interface HarborEvent {
  event: string;
  download_id?: string;
  downloaded_bytes?: number;
  total_bytes?: number | null;
  speed_bps?: number;
  average_bps?: number;
  eta_secs?: number | null;
  active_connections?: number;
  accelerated?: boolean;
  status?: DownloadStatus;
  segments?: Segment[];
  error?: string | null;
  warning?: string | null;
  url?: string;
  title?: string;
  body?: string;
}
