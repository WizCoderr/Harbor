#!/bin/sh
# Build harbor-agent and stage it as a Tauri sidecar for the current target.
set -eu
cd "$(dirname "$0")/.."
root="$(cd ../.. && pwd)"
target="${HARBOR_RUST_TARGET:-}"
if [ -z "$target" ]; then
  target="$(rustc -vV | sed -n 's/^host: //p')"
  cargo build -p harbor-agent --release --manifest-path "$root/Cargo.toml"
  bin="$root/target/release/harbor-agent"
else
  cargo build -p harbor-agent --release --target "$target" --manifest-path "$root/Cargo.toml"
  bin="$root/target/$target/release/harbor-agent"
fi
case "$target" in
  *windows*)
    bin="${bin}.exe"
    suffix=".exe"
    ;;
  *)
    suffix=""
    ;;
esac
mkdir -p src-tauri/binaries
cp "$bin" "src-tauri/binaries/harbor-agent-${target}${suffix}"
echo "Staged harbor-agent for ${target}"
