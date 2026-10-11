import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  createChart,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import { fetchCandles, toPrice } from "../api/client";
import type { TapePrint } from "../hooks/useMarketSocket";

interface Props {
  /** Registers a callback the socket hook calls with each batch of prints. */
  bindTradeSink: (sink: (prints: TapePrint[]) => void) => void;
}

/**
 * History comes from PostgreSQL via the gateway; the live bar is folded from
 * the trade tape in the browser, so the chart advances every print instead of
 * waiting on a database round trip.
 */
export function CandleChart({ bindTradeSink }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const current = useRef<CandlestickData<UTCTimestamp> | null>(null);

  useEffect(() => {
    if (!container.current) return;

    const instance = createChart(container.current, {
      layout: {
        background: { color: "#0d1117" },
        textColor: "#8b949e",
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: "#161b22" },
        horzLines: { color: "#161b22" },
      },
      rightPriceScale: { borderColor: "#21262d" },
      timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: true },
      crosshair: { mode: 0 },
      autoSize: true,
    });

    const candles = instance.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
      borderVisible: false,
    });

    chart.current = instance;
    series.current = candles;

    fetchCandles(600).then((history) => {
      if (!series.current || history.length === 0) return;
      series.current.setData(
        history.map((c) => ({
          time: c.time as UTCTimestamp,
          open: toPrice(c.open),
          high: toPrice(c.high),
          low: toPrice(c.low),
          close: toPrice(c.close),
        })),
      );
      const last = history[history.length - 1];
      current.current = {
        time: last.time as UTCTimestamp,
        open: toPrice(last.open),
        high: toPrice(last.high),
        low: toPrice(last.low),
        close: toPrice(last.close),
      };
      instance.timeScale().scrollToRealTime();
    });

    bindTradeSink((prints) => {
      if (!series.current || prints.length === 0) return;
      const second = Math.floor(Date.now() / 1000) as UTCTimestamp;

      for (const print of prints) {
        const price = toPrice(print.price);
        const bar = current.current;
        if (!bar || bar.time !== second) {
          current.current = {
            time: second,
            open: bar ? bar.close : price,
            high: price,
            low: price,
            close: price,
          };
        } else {
          bar.high = Math.max(bar.high, price);
          bar.low = Math.min(bar.low, price);
          bar.close = price;
        }
      }
      if (current.current) series.current.update(current.current);
    });

    return () => {
      instance.remove();
      chart.current = null;
      series.current = null;
    };
  }, [bindTradeSink]);

  return <div className="chart" ref={container} />;
}
