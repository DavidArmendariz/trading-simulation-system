import { useEffect, useRef, useState } from "react";
import { resetBook, startSimulation, stopSimulation, updateConfig } from "../api/client";

interface Props {
  running: boolean;
  agentCount: number;
  targetTps: number;
  onStateChange: (state: { running: boolean; agent_count: number; target_tps: number }) => void;
}

const TPS_MIN = 100;
const TPS_MAX = 50_000;

// Target TPS spans nearly three orders of magnitude, so the slider is log scale:
// the low end stays adjustable instead of collapsing into the first pixel.
const sliderToTps = (v: number) =>
  Math.round(TPS_MIN * Math.pow(TPS_MAX / TPS_MIN, v / 1000));
const tpsToSlider = (tps: number) =>
  Math.round((Math.log(tps / TPS_MIN) / Math.log(TPS_MAX / TPS_MIN)) * 1000);

export function ControlPanel({ running, agentCount, targetTps, onStateChange }: Props) {
  const [agents, setAgents] = useState(agentCount);
  const [tps, setTps] = useState(targetTps);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const debounce = useRef<ReturnType<typeof setTimeout>>();
  const dirty = useRef(false);

  // Adopt server state unless the operator is mid-drag.
  useEffect(() => {
    if (!dirty.current) {
      setAgents(agentCount);
      setTps(targetTps);
    }
  }, [agentCount, targetTps]);

  const pushConfig = (next: { agent_count: number; target_tps: number }) => {
    if (debounce.current) clearTimeout(debounce.current);
    debounce.current = setTimeout(async () => {
      dirty.current = false;
      try {
        const res = await updateConfig(next);
        onStateChange(res.state);
        setError(null);
      } catch (e) {
        setError(String(e));
      }
    }, 350);
  };

  const onAgents = (value: number) => {
    dirty.current = true;
    setAgents(value);
    pushConfig({ agent_count: value, target_tps: tps });
  };

  const onTps = (value: number) => {
    dirty.current = true;
    setTps(value);
    pushConfig({ agent_count: agents, target_tps: value });
  };

  const run = async (action: () => Promise<{ state: any }>) => {
    setBusy(true);
    setError(null);
    try {
      const res = await action();
      onStateChange(res.state);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="panel">
      <h2>Simulation Control</h2>

      <div className="buttons">
        <button
          className="start"
          disabled={busy || running}
          onClick={() => run(() => startSimulation({ agent_count: agents, target_tps: tps }))}
        >
          Start Simulation
        </button>
        <button className="stop" disabled={busy || !running} onClick={() => run(stopSimulation)}>
          Stop Simulation
        </button>
        <button
          className="ghost"
          disabled={busy}
          onClick={async () => {
            await resetBook();
          }}
          title="Clear the order book without touching the agent pool"
        >
          Reset Book
        </button>
      </div>

      <label className="slider">
        <span>
          Target TPS <strong>{tps.toLocaleString()}</strong>
        </span>
        <input
          type="range"
          min={0}
          max={1000}
          value={tpsToSlider(tps)}
          onChange={(e) => onTps(sliderToTps(Number(e.target.value)))}
        />
      </label>

      <label className="slider">
        <span>
          Active Agents <strong>{agents.toLocaleString()}</strong>
        </span>
        <input
          type="range"
          min={10}
          max={4000}
          step={10}
          value={agents}
          onChange={(e) => onAgents(Number(e.target.value))}
        />
      </label>

      <p className="hint">
        Mix is fixed at 70% noise, 20% momentum, 10% mean reversion. Changing either slider while
        running restarts the agent pool.
      </p>
      {error && <p className="error">{error}</p>}
    </div>
  );
}
