const api = globalThis.chrome ?? globalThis.browser;
const HOST = "com.harbor.download";
const FILE = /\.(zip|rar|7z|tar|gz|bz2|xz|iso|exe|msi|dmg|pkg|deb|rpm|apk|pdf|docx?|xlsx?|pptx?|txt|csv|mp4|mkv|webm|mov|avi|mp3|flac|wav|aac|m4a|ogg|bin)$/i;

api.runtime.onInstalled.addListener(() => {
  api.contextMenus.create({ id: "harbor-link", title: "Download with Harbor", contexts: ["link"] });
  api.contextMenus.create({ id: "harbor-selection", title: "Download selected links with Harbor", contexts: ["selection"] });
  api.contextMenus.create({ id: "harbor-page", title: "Download all file links with Harbor", contexts: ["page"] });
});

api.contextMenus.onClicked.addListener(async (info, tab) => {
  if (info.menuItemId === "harbor-link" && info.linkUrl) {
    await enqueue(info.linkUrl, info.linkUrl.split("/").pop(), info.pageUrl);
    return;
  }
  if (!tab?.id) return;
  const mode = info.menuItemId === "harbor-selection" ? "selection" : "all";
  const response = await api.tabs.sendMessage(tab.id, { type: "collect", mode });
  const urls = response?.urls ?? [];
  await send({ action: "enqueue_many", items: urls.map((url) => ({ url, referrer: info.pageUrl, start: true })) });
});

api.downloads.onCreated.addListener(async (item) => {
  const stored = await api.storage.local.get({ intercept: true });
  if (!stored.intercept || !item.url || item.url.startsWith("blob:") || item.url.startsWith("data:")) return;
  try {
    const cookies = await cookieHeader(item.finalUrl || item.url);
    await send({
      action: "enqueue",
      url: item.finalUrl || item.url,
      filename: item.filename,
      referrer: item.referrer,
      cookies,
      start: true,
    });
    await api.downloads.cancel(item.id);
  } catch (_error) {
    // Leave the browser download in place when Harbor cannot accept it.
  }
});

async function enqueue(url, filename, referrer) {
  const cookies = await cookieHeader(url);
  await send({ action: "enqueue", url, filename, referrer, cookies, start: true });
}

async function cookieHeader(url) {
  try {
    const cookies = await api.cookies.getAll({ url });
    return cookies.map((cookie) => `${cookie.name}=${cookie.value}`).join("; ");
  } catch (_error) {
    return "";
  }
}

function send(message) {
  return new Promise((resolve, reject) => {
    api.runtime.sendNativeMessage(HOST, message, (response) => {
      const error = api.runtime.lastError;
      if (error || !response?.ok) reject(new Error(error?.message || response?.error || "Harbor did not accept the download"));
      else resolve(response);
    });
  });
}

globalThis.collectFileLinks = FILE;
