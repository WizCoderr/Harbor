import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { HarborEvent } from "./types";

export function call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
  return invoke<T>("agent_call", { method, params });
}

export function onEvent(handler: (event: HarborEvent) => void): Promise<UnlistenFn> {
  return listen<HarborEvent>("harbor://event", (message) => handler(message.payload));
}
