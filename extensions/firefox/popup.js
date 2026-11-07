const api = globalThis.chrome ?? globalThis.browser;
const status = document.querySelector("#status");
const intercept = document.querySelector("#intercept");

api.storage.local.get({ intercept: true }).then((stored) => {
  intercept.checked = stored.intercept !== false;
});

intercept.addEventListener("change", () => {
  api.storage.local.set({ intercept: intercept.checked });
});

document.querySelector("#ping").addEventListener("click", () => {
  api.runtime.sendNativeMessage("com.harbor.download", { action: "ping" }, (response) => {
    const error = api.runtime.lastError;
    status.textContent = error ? error.message : response?.ok ? `Connected to Harbor ${response.version}` : "Harbor did not answer";
  });
});
