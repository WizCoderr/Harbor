import type { Download, Segment } from "../types";
import { bytes, eta, speed } from "../format";

export function Details({ download, segments }: { download: Download | null; segments: Segment[] }) {
  if (!download) {
    return (
      <section className="details">
        <div className="empty"><div>Select a download to see its URL, segments, and errors.</div></div>
      </section>
    );
  }
  const bars = segments.length
    ? segments
    : [{ id: "single", start_byte: 0, end_byte: download.total_bytes ?? download.downloaded_bytes, received_bytes: download.downloaded_bytes, status: download.status }];
  return (
    <section className="details" aria-label="Download details">
      <div>
        <h2>{download.filename}</h2>
        <dl className="meta">
          <dt>URL</dt><dd title={download.url}>{download.final_url || download.url}</dd>
          <dt>Destination</dt><dd>{download.destination_dir}</dd>
          <dt>Size</dt><dd className="mono">{bytes(download.downloaded_bytes)} / {bytes(download.total_bytes)}</dd>
          <dt>Speed</dt><dd className="mono">{speed(download.speed_bps)} current · {speed(download.average_bps)} average</dd>
          <dt>Remaining</dt><dd>{eta(download.eta_secs)}</dd>
          <dt>Connections</dt><dd>{download.accelerated ? `${download.active_connections} accelerated` : `${download.active_connections || 1} single connection`}</dd>
          <dt>HTTP status</dt><dd>{download.http_status ?? "—"}</dd>
          <dt>Resume</dt><dd>{download.resume_capable ? "Supported" : "Not available for this server"}</dd>
          <dt>Checksum</dt><dd className="mono">{download.sha256 ?? "—"}</dd>
        </dl>
        {download.error && <p className="error">{download.error}</p>}
        {download.warning && <p>{download.warning}</p>}
      </div>
      <div className="segments">
        <strong>Segments</strong>
        {bars.map((segment, index) => {
          const span = Math.max(1, segment.end_byte - segment.start_byte);
          const ratio = Math.min(100, (segment.received_bytes / span) * 100);
          return (
            <div className="seg" key={segment.id}>
              <span>Part {index + 1}</span>
              <div className="bar"><span style={{ width: `${ratio}%` }} /></div>
              <span className="mono">{Math.round(ratio)}%</span>
            </div>
          );
        })}
      </div>
    </section>
  );
}
