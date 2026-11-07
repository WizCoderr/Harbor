import type { MouseEvent } from "react";
import type { Download } from "../types";
import { bytes, eta, fileType, percent, speed, statusLabel, when } from "../format";

export type SortKey = "filename" | "type" | "size" | "progress" | "speed" | "eta" | "status" | "category" | "added";

export function DownloadTable({
  rows,
  selected,
  sortKey,
  sortDir,
  onSort,
  onSelect,
  onActivate,
  onContext,
}: {
  rows: Download[];
  selected: string[];
  sortKey: SortKey;
  sortDir: "asc" | "desc";
  onSort: (key: SortKey) => void;
  onSelect: (id: string, event: MouseEvent) => void;
  onActivate: (id: string) => void;
  onContext: (id: string, x: number, y: number) => void;
}) {
  const columns: [SortKey, string][] = [
    ["filename", "Filename"],
    ["type", "Type"],
    ["size", "Size"],
    ["progress", "Progress"],
    ["speed", "Speed"],
    ["eta", "Remaining"],
    ["status", "Status"],
    ["category", "Category"],
    ["added", "Added"],
  ];
  if (rows.length === 0) {
    return (
      <div className="empty">
        <div>
          <strong>No downloads in this view</strong>
          Add a URL, drop a link, or import a list.
        </div>
      </div>
    );
  }
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map(([key, label]) => (
              <th key={key} onClick={() => onSort(key)} aria-sort={sortKey === key ? (sortDir === "asc" ? "ascending" : "descending") : "none"}>
                {label}
                {sortKey === key ? (sortDir === "asc" ? " ↑" : " ↓") : ""}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const ratio = percent(row.downloaded_bytes, row.total_bytes);
            return (
              <tr
                key={row.id}
                aria-selected={selected.includes(row.id)}
                onClick={(event) => onSelect(row.id, event)}
                onDoubleClick={() => onActivate(row.id)}
                onContextMenu={(event) => {
                  event.preventDefault();
                  onContext(row.id, event.clientX, event.clientY);
                }}
              >
                <td>{row.filename}</td>
                <td>{fileType(row.filename, row.mime)}</td>
                <td className="mono">{bytes(row.total_bytes)}</td>
                <td className="progress">
                  <div className={ratio == null ? "bar unknown" : "bar"} aria-label={ratio == null ? "Size unknown" : `${Math.round(ratio)} percent`}>
                    <span style={{ width: `${ratio ?? 30}%` }} />
                  </div>
                </td>
                <td className="mono">{speed(row.speed_bps)}</td>
                <td className="mono">{eta(row.eta_secs)}</td>
                <td className={`status ${row.status}`}>{statusLabel(row.status)}</td>
                <td>{row.category_name ?? "—"}</td>
                <td>{when(row.created_at)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
