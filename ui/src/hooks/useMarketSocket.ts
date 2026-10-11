import { useEffect, useRef, useState } from "react";
import { WS_URL, type EngineStats } from "../api/client";

export interface BookLevel {
  price: number;
  qty: number;
  cumulative: number;
}

export interface BookState {
  seq: number;
  bids: BookLevel[];
  asks: BookLevel[];
  last: number;
  bestBid: number | null;
  bestAsk: number | null;
  spread: number | null;
  restingOrders: number;
}

export interface TapePrint {
  id: number;
  price: number;
  qty: number;
  side: number;
}

export interface AgentStatus {
  running: boolean;
  workers: number;
  agent_count: number;
  target_tps: number;
}

const EMPTY_BOOK: BookState = {
  seq: 0,
  bids: [],
  asks: [],
  last: 0,
  bestBid: null,
  bestAsk: null,
  spread: null,
  restingOrders: 0,
};

const TAPE_LENGTH = 40;

function withCumulative(levels: [number, number][]): BookLevel[] {
  let running = 0;
  return levels.map(([price, qty]) => {
    running += qty;
    return { price, qty, cumulative: running };
  });
}

/**
 * One WebSocket, buffered into refs and flushed on requestAnimationFrame.
 * React re-renders at frame rate rather than at message rate, which is what
 * keeps a 60fps feed from turning into thousands of renders per second.
 */
export function useMarketSocket(onTrades?: (prints: TapePrint[]) => void) {
  const [connected, setConnected] = useState(false);
  const [book, setBook] = useState<BookState>(EMPTY_BOOK);
  const [tape, setTape] = useState<TapePrint[]>([]);
  const [stats, setStats] = useState<EngineStats | null>(null);
  const [agents, setAgents] = useState<AgentStatus | null>(null);
  const [gaps, setGaps] = useState(0);

  const pendingBook = useRef<BookState | null>(null);
  const pendingTape = useRef<TapePrint[]>([]);
  const pendingStats = useRef<EngineStats | null>(null);
  const pendingAgents = useRef<AgentStatus | null>(null);
  const lastSeq = useRef(0);
  const tradesCallback = useRef(onTrades);
  tradesCallback.current = onTrades;

  useEffect(() => {
    let socket: WebSocket | null = null;
    let frame = 0;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    let closed = false;

    const flush = () => {
      if (pendingBook.current) {
        setBook(pendingBook.current);
        pendingBook.current = null;
      }
      if (pendingTape.current.length) {
        const batch = pendingTape.current;
        pendingTape.current = [];
        tradesCallback.current?.(batch);
        setTape((prev) => [...batch.reverse(), ...prev].slice(0, TAPE_LENGTH));
      }
      if (pendingStats.current) {
        setStats(pendingStats.current);
        pendingStats.current = null;
      }
      if (pendingAgents.current) {
        setAgents(pendingAgents.current);
        pendingAgents.current = null;
      }
      frame = requestAnimationFrame(flush);
    };

    const connect = () => {
      socket = new WebSocket(WS_URL);

      socket.onopen = () => {
        attempt = 0;
        setConnected(true);
      };

      socket.onclose = () => {
        setConnected(false);
        if (closed) return;
        attempt += 1;
        retry = setTimeout(connect, Math.min(1000 * attempt, 5000));
      };

      socket.onerror = () => socket?.close();

      socket.onmessage = (event) => {
        const message = JSON.parse(event.data);
        switch (message.type) {
          case "book": {
            const d = message.data;
            if (lastSeq.current && d.seq > lastSeq.current + 1) {
              setGaps((g) => g + 1);
            }
            lastSeq.current = d.seq;
            pendingBook.current = {
              seq: d.seq,
              bids: withCumulative(d.bids ?? []),
              asks: withCumulative(d.asks ?? []),
              last: d.last,
              bestBid: d.best_bid ?? null,
              bestAsk: d.best_ask ?? null,
              spread: d.spread ?? null,
              restingOrders: d.resting_orders ?? 0,
            };
            break;
          }
          case "trades":
            pendingTape.current.push(...(message.data.trades ?? []));
            break;
          case "stats":
            pendingStats.current = message.data;
            break;
          case "agents":
            pendingAgents.current = message.data;
            break;
        }
      };
    };

    connect();
    frame = requestAnimationFrame(flush);

    return () => {
      closed = true;
      cancelAnimationFrame(frame);
      if (retry) clearTimeout(retry);
      socket?.close();
    };
  }, []);

  return { connected, book, tape, stats, agents, gaps };
}
