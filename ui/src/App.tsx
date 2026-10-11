import { useCallback, useEffect, useRef, useState } from "react";
import { fetchStatus, toPrice, type SimState } from "./api/client";
import { useMarketSocket, type TapePrint } from "./hooks/useMarketSocket";
import { CandleChart } from "./components/CandleChart";
import { ControlPanel } from "./components/ControlPanel";
import { DepthLadder } from "./components/DepthLadder";
import { StatusBadge } from "./components/StatusBadge";
import { TapeFeed } from "./components/TapeFeed";

export default function App() {
  const [sim, setSim] = useState<SimState>({
    running: false,
    agent_count: 2000,
    target_tps: 5000,
    updated_at_ms: 0,
  });

  // The chart registers a sink so trade batches reach it without a re-render.
  const tradeSink = useRef<((prints: TapePrint[]) => void) | null>(null);
  const bindTradeSink = useCallback((sink: (prints: TapePrint[]) => void) => {
    tradeSink.current = sink;
  }, []);
  const onTrades = useCallback((prints: TapePrint[]) => tradeSink.current?.(prints), []);

  const { connected, book, tape, stats, agents, gaps } = useMarketSocket(onTrades);

  // The agent supervisor is the authority on whether the pool is actually up.
  useEffect(() => {
    if (agents) {
      setSim((prev) => ({ ...prev, running: agents.running }));
    }
  }, [agents]);

  // Poll status for the agent-side send rate, which the supervisor reports
  // per worker and the gateway sums; it is not on the market data stream.
  const [agentSend, setAgentSend] = useState<number | null>(null);
  useEffect(() => {
    let cancelled = false;
    const poll = () =>
      fetchStatus()
        .then((res) => {
          if (cancelled) return;
          if (res.state) setSim((prev) => ({ ...prev, ...res.state }));
          setAgentSend(res.agent_orders_per_sec ?? null);
        })
        .catch(() => undefined);
    poll();
    const timer = setInterval(poll, 3000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  return (
    <div className="app">
      <header>
        <div className="brand">
          <h1>SIMX</h1>
          <span className="price">{book.last ? toPrice(book.last).toFixed(2) : "--"}</span>
        </div>
        <StatusBadge
          running={sim.running}
          connected={connected}
          stats={stats}
          agentOrdersPerSec={agentSend}
          gaps={gaps}
        />
      </header>

      <main>
        <section className="chart-pane">
          <CandleChart bindTradeSink={bindTradeSink} />
        </section>

        <aside className="book-pane">
          <h2>Order Book (L2)</h2>
          <DepthLadder book={book} />
        </aside>

        <section className="control-pane">
          <ControlPanel
            running={sim.running}
            agentCount={sim.agent_count}
            targetTps={sim.target_tps}
            onStateChange={(state) => setSim((prev) => ({ ...prev, ...state }))}
          />
        </section>

        <aside className="tape-pane">
          <h2>Time &amp; Sales</h2>
          <TapeFeed tape={tape} />
        </aside>
      </main>
    </div>
  );
}
