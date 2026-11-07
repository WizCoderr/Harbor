# Public reference notes for Harbor

Harbor is an independent download manager. These notes record the workflows that informed its design. They are drawn from public product documentation, not from an IDM installation or from IDM source code.

## What was available

- The project directory was empty.
- No Internet Download Manager binary was installed on this Mac.
- REA (https://github.com/morluto/rea) is an analysis toolkit. It was not registered in Cursor MCP, and it is not the application framework.
- REA's own documentation describes local binary, application, and website inspection. Using it against an installed desktop app would require that app to be present and would still not permit copying proprietary source or assets. That path was not available, and it is not required to build Harbor.
- No licensing, authentication, or DRM control was bypassed. IDM logos, graphics, and proprietary text are not reused.

## Workflows taken from public IDM documentation

Sources:

- https://www.internetdownloadmanager.com/features2.html
- https://www.internetdownloadmanager.com/support/using_idm/using_idm.html
- https://www.internetdownloadmanager.com/support/main.html
- https://www.internetdownloadmanager.com/support/idm-scheduler/idm_queues.html
- https://www.internetdownloadmanager.com/support/idm-scheduler/idm_scheduler.html
- https://www.internetdownloadmanager.com/support/schedulerD.html

### Main window

The public main-dialog guide describes a category tree on the left, a toolbar whose buttons enable or disable based on the current selection, and a download list. A queue marker shows whether a file belongs to a queue. Harbor keeps that workflow and uses an original layout.

### Add URL

The documented flow is: provide a URL, choose a file name and folder, then start immediately or add the file for later. Harbor's Add URL dialog probes the response headers first so the name and size can be edited before the transfer starts.

### Dynamic segmentation

IDM's public feature page describes dynamic segmentation as follows: the file is not divided into a fixed set of pieces only once at the start. When another connection is available, the largest remaining piece is split in half and the new connection downloads from that midpoint. Harbor implements that idea independently:

- A one-byte ranged probe checks whether the server actually returns `206`, `Content-Range`, and a known length.
- Segmented mode is used only when that probe succeeds and the file is at least 2 MiB, with more than one connection allowed.
- Each connection owns an exclusive byte range. A new connection can take only the unlocked tail of the largest range, past bytes already reserved by the connection that is fetching it.
- If the server answers `200` instead of `206`, Harbor switches to a single stream and does not describe the download as accelerated.

### Resume

The public docs say unfinished downloads continue after a dropped connection, a restart, or a shutdown. Harbor stores segment progress in SQLite and keeps a `.harborpart` file. Resume sends `Range` with `If-Range` when an `ETag` or `Last-Modified` value is known. A `206` continues the partial file. A `200` means the remote file changed, so the partial file is discarded and the transfer starts over. `401`, `403`, `404`, and `410` are reported as authorization or availability errors and the partial file is left in place until the user deletes it.

### Categories

Documented categories have a name, a default directory, and a list of file extensions. A category can also be limited to selected sites. Matches choose the folder automatically. Harbor ships Videos, Music, Documents, Compressed, and Applications, and users can add their own.

### Queues and the scheduler

The public scheduler docs describe:

- A main download queue and a synchronization queue, plus extra queues.
- Per-queue concurrency and a user-controlled order.
- Manual start and stop, a start time, and a stop time.
- One-time download queues remove a file from the queue when it completes.
- Synchronization queues keep their files and download again only when the remote file changed.
- A file that fails goes to the end of the queue and is retried. A queue with only a failing file retries about every 30 seconds.

Harbor follows that behavior. The desktop window is not required to be open: the headless agent runs due work, and the app registers a one-shot operating-system timer that launches `harbor-agent --run-due`.

### Browser capture

The public feature list describes a browser extension that can take over a normal download, a context-menu action, and adding every link on a page. Harbor's extension sends the URL, file name hint, referrer, and cookies for that site to the local agent. Interception can be turned off. If the agent cannot be reached, the browser keeps the download.

Manifest V3 does not give an extension reliable access to every original request header. Harbor forwards the URL, referrer, and cookies, plus any headers the extension is actually allowed to see.

### Other documented behavior Harbor implements

- Basic authentication, stored in the operating system keychain.
- Custom request headers, with authorization and cookie values kept out of the database.
- Proxy URL, bandwidth limit, and an hourly byte quota.
- Clipboard URL monitoring and drag-and-drop, as optional ways to open Add URL.
- A grabber that collects links from a page the user names, with extension, domain, and depth filters.
- An optional antivirus command run against a finished file. Harbor does not bundle a scanner.
- Light and dark themes.

## Behavior Harbor does not copy

- IDM registration, licensing, and update servers.
- MMS downloads.
- NTLM, Negotiate, and Kerberos. Harbor uses Basic authentication and user-supplied headers.
- DRM or signature-protected streaming. A normal `http`, `https`, or `ftp` media file is downloaded as a file. Protected players are not bypassed.
- System-wide hooks that capture downloads from every application.
- Modem hang-up and forced process termination.
- IDM skins, icons, and branding.
- Multi-connection FTP. FTP is one stream and can resume with `REST` when the server allows it.
