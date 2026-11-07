const api = globalThis.chrome ?? globalThis.browser;
const FILE = /\.(zip|rar|7z|tar|gz|bz2|xz|iso|exe|msi|dmg|pkg|deb|rpm|apk|pdf|docx?|xlsx?|pptx?|txt|csv|mp4|mkv|webm|mov|avi|mp3|flac|wav|aac|m4a|ogg|bin)$/i;

api.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type !== "collect") return;
  const anchors = [...document.querySelectorAll("a[href]")];
  const selected = message.mode === "selection" ? selectionAnchors() : anchors;
  const urls = selected
    .map((anchor) => anchor.href)
    .filter((href) => href && (FILE.test(href.split("?")[0]) || anchor.hasAttribute("download")));
  sendResponse({ urls: [...new Set(urls)] });
  return true;
});

function selectionAnchors() {
  const selection = window.getSelection();
  if (!selection || selection.rangeCount === 0) return [];
  const range = selection.getRangeAt(0);
  return [...document.querySelectorAll("a[href]")].filter((anchor) => selection.containsNode(anchor, true) || range.intersectsNode(anchor));
}
