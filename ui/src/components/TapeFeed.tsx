import { toPrice } from "../api/client";
import type { TapePrint } from "../hooks/useMarketSocket";

/** Time and sales. `side` is the aggressor: 0 = buyer lifted, 1 = seller hit. */
export function TapeFeed({ tape }: { tape: TapePrint[] }) {
  return (
    <div className="tape">
      <div className="tape-header">
        <span>Price</span>
        <span>Size</span>
        <span>Side</span>
      </div>
      <div className="tape-rows">
        {tape.map((print) => (
          <div key={print.id} className={`tape-row ${print.side === 0 ? "buy" : "sell"}`}>
            <span>{toPrice(print.price).toFixed(2)}</span>
            <span>{print.qty.toLocaleString()}</span>
            <span>{print.side === 0 ? "BUY" : "SELL"}</span>
          </div>
        ))}
        {tape.length === 0 && <div className="empty">no prints yet</div>}
      </div>
    </div>
  );
}
