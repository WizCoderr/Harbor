# Harbor

Harbor is a free, MIT-licensed download manager for macOS, Windows, Linux, and Android. Every feature is included. There is no paid upgrade, registration key, or locked “full version.”

It downloads real HTTP, HTTPS, and FTP files. When a server accepts byte ranges and the file is at least 256 KiB, Harbor opens up to 16 connections and keeps splitting the largest remaining piece. Partial files survive a pause, a dropped network, or a restart.

The interface follows the workflows people expect from a download manager: add a URL, choose a name and folder, queue it, schedule it, and watch segment progress. The visual design is Harbor's own.

## Layout

- `crates/harbor-core` — shared records, segment splitting, and schedule math
- `crates/harbor-store` — SQLite storage
- `crates/harbor-engine` — HTTP, HTTPS, and FTP transfers
- `crates/harbor-agent` — headless process, local socket, scheduler wakeup, native messaging
- `apps/desktop` — Tauri window
- `extensions/chromium` — Chrome, Edge, Brave, and other Chromium browsers
- `extensions/firefox` — Firefox
- `docs/research/idm-public-reference.md` — public workflow notes

The window does not download files itself. It talks to `harbor-agent` over a local socket. The same agent serves browser native messaging, so a scheduled queue can run when the window is closed.

## License

Harbor is released under the [MIT License](LICENSE). You can use, modify, and share it at no cost.

## Platforms

Run the same commands on the operating system you want to ship:

| System | Package |
| --- | --- |
| macOS | `target/release/bundle/macos/Harbor.app` and a `.dmg` |
| Windows | `target/release/bundle/msi` and `nsis` |
| Linux | `target/release/bundle/deb` and `appimage` |
| Android | an APK from Tauri’s Android build, after Android Studio is installed |

Desktop builds need Rust, Node.js 22 or newer, and pnpm. Linux also needs the WebKitGTK development packages. Android needs Android Studio, an SDK, and the Rust target `aarch64-linux-android`.

```bash
cargo test --workspace
cargo build -p harbor-agent --release
cd apps/desktop
pnpm install
pnpm tauri build
```

On Android, from `apps/desktop`:

```bash
pnpm tauri android init
pnpm tauri android build
```

Browser integration is for desktop Chrome, Edge, Brave, and Firefox. Android uses the Harbor window itself.

The release agent is `target/release/harbor-agent`. Desktop bundles are under `target/release/bundle`. On macOS the app folder also contains `harbor-agent` so the window can start downloads.

## Development

```bash
cargo build -p harbor-agent
cd apps/desktop
pnpm install
pnpm tauri dev
```

`pnpm tauri dev` starts the Vite window and the Tauri shell. The shell starts `harbor-agent` if it is not already listening. Set `HARBOR_AGENT` to point at a specific agent binary.

Engine tests use a local HTTP server and do not touch the network:

```bash
cargo test -p harbor-engine --test download
```

## Browser extension

The extension id for Chromium is `kgfejgpbkjaiaohkoofijjdkonojhikn`. Firefox uses `harbor@harbor.local`.

1. Build or run `harbor-agent`.
2. Install the native host from that binary:

```bash
./target/debug/harbor-agent --install-host
```

3. Chrome, Edge, or Brave: open `chrome://extensions` or `edge://extensions`, turn on Developer mode, and load `extensions/chromium`.
4. Firefox: open `about:debugging#/runtime/this-firefox` and load `extensions/firefox`.

The popup can turn interception off. A context menu can send one link, the selected links, or the file links on the page. If the agent cannot accept a URL, the browser keeps its normal download.

Manifest V3 does not give the extension every original request header. Harbor receives the URL, the file name hint, the page referrer, and the cookies for that URL.

## Data and secrets

The database is in the platform data directory, in a `Harbor` folder. Partial files use the suffix `.harborpart` and are renamed only when the transfer finishes. An existing completed file is not overwritten unless the item belongs to a synchronization queue.

Usernames, passwords, cookies, and authorization headers are stored in the operating system keychain on macOS, Windows, and Linux. On Android they stay in the app’s private storage. They are not written to the download database and they are not logged.

## What Harbor does not do

- It does not use IDM's name, logo, or graphics.
- It does not bypass DRM, account walls, or signature-protected video.
- It does not speak MMS, NTLM, Negotiate, or Kerberos. Authentication is HTTP Basic plus headers and cookies you supply.
- FTP is one connection and resumes with `REST` when the server allows it.
- A direct audio or video file URL is downloaded as a file. Protected players are left alone.
- Update checks read a JSON manifest when you configure one. Harbor does not install the new build for you.
- An antivirus command, if set, is an executable path. Harbor does not ship a scanner.
