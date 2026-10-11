import type { EngineStats } from "../api/client";

interface Props {
  running: boolean;
  connected: boolean;
  stats: EngineStats | null;
  agentOrdersPerSec: number | null;
  gaps: number;
}

const fmt = (n: number | undefined) => (n ?? 0).toLocaleString();

export function StatusBadge({ running, connected, stats, agentOrdersPerSec, gaps }: Props) {
  return (
    <div className="status">
      <span className={`pill ${running ? "on" : "off"}`}>{running ? "RUNNING" : "STOPPED"}</span>
      <span className={`pill ${connected ? "link" : "off"}`}>
        {connected ? "WS LIVE" : "WS DOWN"}
      </span>

      <Metric label="orders/s" value={fmt(stats?.orders_per_sec)} />
      <Metric label="trades/s" value={fmt(stats?.trades_per_sec)} />
      <Metric label="resting" value={fmt(stats?.resting_orders)} />
      <Metric label="volume" value={fmt(stats?.volume)} />
      {agentOrdersPerSec !== null && <Metric label="agent send/s" value={fmt(agentOrdersPerSec)} />}
      {(stats?.orders_dropped ?? 0) > 0 && (
        <Metric label="dropped" value={fmt(stats?.orders_dropped)} warn />
      )}
      {gaps > 0 && <Metric label="seq gaps" value={String(gaps)} warn />}
    </div>
  );
}

function Metric({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <span className={`metric ${warn ? "warn" : ""}`}>
      <em>{label}</em>
      <strong>{value}</strong>
    </span>
  );
}
