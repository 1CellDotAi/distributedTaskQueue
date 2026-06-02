import { useEffect, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, wsURL, type Task } from "./api";

type Tab = "overview" | "tasks" | "workers" | "dlq" | "submit";

export function App() {
  const [tab, setTab] = useState<Tab>("overview");
  const live = useLiveEvents();

  return (
    <div className="container">
      <header>
        <h1>⚡ Task Queue Dashboard</h1>
        <div className="tabs">
          {(["overview", "tasks", "workers", "dlq", "submit"] as Tab[]).map((t) => (
            <button key={t} className={tab === t ? "active" : ""} onClick={() => setTab(t)}>
              {t}
            </button>
          ))}
        </div>
        <span className={`live ${live.connected ? "on" : ""}`}>
          <span className="dot" /> {live.connected ? `live (${live.count} events)` : "offline"}
        </span>
      </header>
      {tab === "overview" && <Overview />}
      {tab === "tasks" && <Tasks />}
      {tab === "workers" && <Workers />}
      {tab === "dlq" && <DLQ />}
      {tab === "submit" && <Submit />}
    </div>
  );
}

function useLiveEvents() {
  const qc = useQueryClient();
  const [connected, setConnected] = useState(false);
  const [count, setCount] = useState(0);
  useEffect(() => {
    let ws: WebSocket | null = null;
    let stop = false;
    const connect = () => {
      ws = new WebSocket(wsURL());
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!stop) setTimeout(connect, 1500);
      };
      ws.onmessage = () => {
        setCount((c) => c + 1);
        qc.invalidateQueries();
      };
    };
    connect();
    return () => {
      stop = true;
      ws?.close();
    };
  }, [qc]);
  return { connected, count };
}

function Overview() {
  const { data: stats } = useQuery({ queryKey: ["stats"], queryFn: api.stats });
  if (!stats) return <div className="card">Loading...</div>;
  const byStatus = stats.tasks.by_status || {};
  const total = stats.tasks.total || 0;
  const dead = byStatus["dead"] || 0;
  const failureRate = total > 0 ? ((dead / total) * 100).toFixed(2) : "0.00";
  return (
    <div>
      <div className="card">
        <h3>Tasks</h3>
        <div className="grid">
          <Stat label="Total" value={total} />
          {Object.entries(byStatus).map(([k, v]) => (
            <Stat key={k} label={k} value={v} />
          ))}
          <Stat label="Failure rate" value={`${failureRate}%`} />
          <Stat label="WS clients" value={stats.ws_clients} />
        </div>
      </div>
      <div className="card">
        <h3>Queue depths</h3>
        <table>
          <thead>
            <tr>
              <th>Type</th>
              <th>Ready</th>
              <th>Delayed</th>
              <th>DLQ</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(stats.queues || {}).map(([t, d]) => (
              <tr key={t}>
                <td>{t}</td>
                <td>{d.ready}</td>
                <td>{d.delayed}</td>
                <td>{d.dlq}</td>
              </tr>
            ))}
            {Object.keys(stats.queues || {}).length === 0 && (
              <tr>
                <td colSpan={4} style={{ color: "#9aa5c1" }}>No queues yet — submit a task.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="stat">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

function Tasks() {
  const [status, setStatus] = useState("");
  const [type, setType] = useState("");
  const { data } = useQuery({ queryKey: ["tasks", status, type], queryFn: () => api.listTasks(status, type) });
  const [open, setOpen] = useState<Task | null>(null);
  const cancel = useMutation({ mutationFn: (id: string) => api.cancel(id) });
  return (
    <div>
      <div className="card">
        <div style={{ display: "flex", gap: 8 }}>
          <select value={status} onChange={(e) => setStatus(e.target.value)} style={{ maxWidth: 200 }}>
            <option value="">all statuses</option>
            {["queued", "running", "retrying", "succeeded", "failed", "dead", "canceled"].map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <input placeholder="type filter (e.g. echo)" value={type} onChange={(e) => setType(e.target.value)} />
        </div>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Type</th>
              <th>Pri</th>
              <th>Status</th>
              <th>Attempts</th>
              <th>Worker</th>
              <th>Updated</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {(data || []).map((t) => (
              <tr key={t.id}>
                <td><code>{t.id.slice(0, 8)}</code></td>
                <td>{t.type}</td>
                <td>{t.priority}</td>
                <td><span className={`badge b-${t.status}`}>{t.status}</span></td>
                <td>{t.attempts}/{t.max_attempts}</td>
                <td>{t.worker_id || "-"}</td>
                <td>{new Date(t.updated_at).toLocaleTimeString()}</td>
                <td>
                  <button className="secondary" onClick={() => setOpen(t)}>view</button>
                  {(t.status === "queued" || t.status === "retrying") && (
                    <button className="secondary" onClick={() => cancel.mutate(t.id)}>cancel</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {open && (
        <div className="card">
          <h3>Task {open.id}</h3>
          <pre>{JSON.stringify(open, null, 2)}</pre>
          <button className="secondary" onClick={() => setOpen(null)}>close</button>
        </div>
      )}
    </div>
  );
}

function Workers() {
  const { data } = useQuery({ queryKey: ["workers"], queryFn: api.workers });
  return (
    <div className="card">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Hostname</th>
            <th>Status</th>
            <th>Last heartbeat</th>
            <th>Age</th>
          </tr>
        </thead>
        <tbody>
          {(data || []).map((w) => {
            const age = (Date.now() - new Date(w.last_heartbeat).getTime()) / 1000;
            return (
              <tr key={w.id}>
                <td><code>{w.id}</code></td>
                <td>{w.hostname}</td>
                <td><span className={`badge b-${w.status === "alive" ? "running" : "dead"}`}>{w.status}</span></td>
                <td>{new Date(w.last_heartbeat).toLocaleTimeString()}</td>
                <td>{age.toFixed(1)}s</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function DLQ() {
  const { data } = useQuery({ queryKey: ["dlq"], queryFn: api.dlq });
  const redrive = useMutation({ mutationFn: (id: string) => api.redrive(id) });
  return (
    <div className="card">
      <table>
        <thead>
          <tr>
            <th>Task ID</th>
            <th>Reason</th>
            <th>Failed at</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {(data || []).map((d) => (
            <tr key={d.id}>
              <td><code>{d.task_id.slice(0, 8)}</code></td>
              <td>{d.reason}</td>
              <td>{new Date(d.failed_at).toLocaleTimeString()}</td>
              <td><button className="secondary" onClick={() => redrive.mutate(d.task_id)}>redrive</button></td>
            </tr>
          ))}
          {!data?.length && (
            <tr><td colSpan={4} style={{ color: "#9aa5c1" }}>No dead-letter tasks.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function Submit() {
  const [type, setType] = useState("echo");
  const [priority, setPriority] = useState(5);
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [payload, setPayload] = useState(`{"hello":"world"}`);
  const [msg, setMsg] = useState<string>("");
  const submit = useMutation({
    mutationFn: () => {
      let parsed: unknown = {};
      try {
        parsed = JSON.parse(payload || "{}");
      } catch {
        throw new Error("payload must be valid JSON");
      }
      return api.createTask({ type, payload: parsed, priority, max_attempts: maxAttempts });
    },
    onSuccess: (t) => setMsg(`Created ${t.id}`),
    onError: (e: Error) => setMsg(e.message),
  });
  return (
    <div className="card">
      <h3>Submit Task</h3>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          submit.mutate();
        }}
      >
        <div className="row">
          <label>Type</label>
          <input value={type} onChange={(e) => setType(e.target.value)} />
        </div>
        <div className="row">
          <label>Priority (0=high, 9=low)</label>
          <input type="number" min={0} max={9} value={priority} onChange={(e) => setPriority(Number(e.target.value))} />
        </div>
        <div className="row">
          <label>Max attempts</label>
          <input type="number" min={1} max={20} value={maxAttempts} onChange={(e) => setMaxAttempts(Number(e.target.value))} />
        </div>
        <div className="row">
          <label>Payload (JSON)</label>
          <textarea rows={5} value={payload} onChange={(e) => setPayload(e.target.value)} />
        </div>
        <button className="primary" type="submit">Submit</button>
        {msg && <p style={{ marginTop: 12 }}>{msg}</p>}
      </form>
    </div>
  );
}
