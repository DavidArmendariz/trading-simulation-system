const HTTP_BASE =
  import.meta.env.VITE_GATEWAY_HTTP ?? `http://${window.location.hostname}:18080`;
export const WS_URL =
  (import.meta.env.VITE_GATEWAY_WS ?? `ws://${window.location.hostname}:18080`) + "/ws";

/** All prices on the wire are integer ticks (cents). Display units live here. */
export const TICKS_PER_UNIT = 100;
export const toPrice = (ticks: number) => ticks / TICKS_PER_UNIT;

export interface SimState {
  running: boolean;
  agent_count: number;
  target_tps: number;
  updated_at_ms: number;
}

export interface EngineStats {
  orders_in: number;
  trades_out: number;
  volume: number;
  orders_per_sec: number;
  trades_per_sec: number;
  resting_orders: number;
  orders_dropped: number;
  snapshots_dropped: number;
}

export interface Candle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${HTTP_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${path} failed: ${res.status}`);
  return res.json() as Promise<T>;
}

export const startSimulation = (config?: { agent_count?: number; target_tps?: number }) =>
  post<{ state: SimState }>("/api/v1/simulation/start", config);

export const stopSimulation = () => post<{ state: SimState }>("/api/v1/simulation/stop");

export const updateConfig = (config: { agent_count?: number; target_tps?: number }) =>
  post<{ state: SimState }>("/api/v1/simulation/config", config);

export const resetBook = () => post<{ status: string }>("/api/v1/simulation/reset");

export interface StatusResponse {
  symbol: string;
  state: SimState;
  engine: EngineStats | null;
  agent_orders_per_sec: number;
  ws_clients: number;
  trades_persisted: number;
}

export async function fetchStatus(): Promise<StatusResponse> {
  const res = await fetch(`${HTTP_BASE}/api/v1/simulation/status`);
  if (!res.ok) throw new Error(`status failed: ${res.status}`);
  return res.json();
}

export async function fetchCandles(limit = 600): Promise<Candle[]> {
  const res = await fetch(`${HTTP_BASE}/api/v1/candles?limit=${limit}`);
  if (!res.ok) return [];
  const body = await res.json();
  return (body.candles ?? []) as Candle[];
}
