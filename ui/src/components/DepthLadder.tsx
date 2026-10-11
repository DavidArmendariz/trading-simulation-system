import { toPrice } from "../api/client";
import type { BookState } from "../hooks/useMarketSocket";

interface Props {
  book: BookState;
  depth?: number;
}

/** Level 2 DOM ladder. Asks descend to the spread, bids run below it. */
export function DepthLadder({ book, depth = 12 }: Props) {
  const asks = book.asks.slice(0, depth);
  const bids = book.bids.slice(0, depth);
  const maxCumulative = Math.max(
    asks.at(-1)?.cumulative ?? 0,
    bids.at(-1)?.cumulative ?? 0,
    1,
  );

  return (
    <div className="ladder">
      <div className="ladder-header">
        <span>Price</span>
        <span>Size</span>
        <span>Total</span>
      </div>

      <div className="ladder-side">
        {[...asks].reverse().map((level) => (
          <Row key={`a${level.price}`} level={level} side="ask" max={maxCumulative} />
        ))}
      </div>

      <div className="ladder-spread">
        <span className="last">{book.last ? toPrice(book.last).toFixed(2) : "--"}</span>
        <span className="spread">
          {book.spread !== null ? `spread ${toPrice(book.spread).toFixed(2)}` : "no spread"}
        </span>
      </div>

      <div className="ladder-side">
        {bids.map((level) => (
          <Row key={`b${level.price}`} level={level} side="bid" max={maxCumulative} />
        ))}
      </div>
    </div>
  );
}

function Row({
  level,
  side,
  max,
}: {
  level: { price: number; qty: number; cumulative: number };
  side: "bid" | "ask";
  max: number;
}) {
  const width = `${Math.min(100, (level.cumulative / max) * 100)}%`;
  return (
    <div className={`ladder-row ${side}`}>
      <div className="depth-bar" style={{ width }} />
      <span className="price">{toPrice(level.price).toFixed(2)}</span>
      <span>{level.qty.toLocaleString()}</span>
      <span className="muted">{level.cumulative.toLocaleString()}</span>
    </div>
  );
}
