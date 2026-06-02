export const API_URL =
  (import.meta as unknown as { env: Record<string, string | undefined> }).env
    .VITE_API_URL || "http://localhost:8080";

export type Task = {
  id: string;
  type: string;
  payload: unknown;
  priority: number;
  status: string;
  attempts: number;
  max_attempts: number;
  available_at: string;
  started_at?: string;
  finished_at?: string;
  worker_id?: string;
  result?: unknown;
  last_error?: string;
  created_at: string;
  updated_at: string;
};

export type Worker = {
  id: string;
  hostname: string;
  status: string;
  last_heartbeat: string;
};

export type Stats = {
  tasks: { by_status: Record<string, number>; by_type: Record<string, number>; total: number };
  queues: Record<string, { ready: number; delayed: number; dlq: number }>;
  ws_clients: number;
};

export type DeadLetter = {
  id: number;
  task_id: string;
  reason: string;
  payload: unknown;
  failed_at: string;
};

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(API_URL + path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!r.ok) throw new Error(`${r.status}: ${await r.text()}`);
  return r.json();
}

export const api = {
  listTasks: (status = "", type = "") =>
    req<Task[]>(`/api/tasks?status=${encodeURIComponent(status)}&type=${encodeURIComponent(type)}&limit=100`),
  getTask: (id: string) => req<Task>(`/api/tasks/${id}`),
  createTask: (body: { type: string; payload: unknown; priority: number; max_attempts: number }) =>
    req<Task>(`/api/tasks`, { method: "POST", body: JSON.stringify(body) }),
  cancel: (id: string) => req<unknown>(`/api/tasks/${id}/cancel`, { method: "POST" }),
  workers: () => req<Worker[]>(`/api/workers`),
  stats: () => req<Stats>(`/api/stats`),
  dlq: () => req<DeadLetter[]>(`/api/dlq`),
  redrive: (id: string) => req<Task>(`/api/dlq/${id}/redrive`, { method: "POST" }),
};

export function wsURL(): string {
  return API_URL.replace(/^http/, "ws") + "/ws";
}
