export function bytes(value: number | null | undefined): string {
  if (value == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size >= 10 || unit === 0 ? size.toFixed(unit === 0 ? 0 : 1) : size.toFixed(2)} ${units[unit]}`;
}

export function speed(value: number): string {
  if (!value) return "—";
  const units = ["B/s", "KB/s", "MB/s", "GB/s", "TB/s"];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  if (unit === 0) return `${Math.round(size)} B/s`;
  return `${size.toFixed(1)} ${units[unit]}`;
}

export function eta(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return "—";
  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  const pad = (value: number) => String(value).padStart(2, "0");
  if (hours > 0) return `${hours}:${pad(minutes)}:${pad(secs)}`;
  return `${minutes}:${pad(secs)}`;
}

export function when(millis: number): string {
  return new Date(millis).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function percent(done: number, total: number | null): number | null {
  if (!total || total <= 0) return null;
  return Math.max(0, Math.min(100, (done / total) * 100));
}

export function fileType(filename: string, mime: string | null): string {
  const ext = filename.split(".").pop()?.toUpperCase();
  if (ext && ext !== filename.toUpperCase()) return ext;
  return mime?.split("/")[1]?.toUpperCase() ?? "FILE";
}

export function statusLabel(status: string): string {
  return status.replaceAll("_", " ");
}
