"""
indicator_alerts.py
────────────────────
Python port of algo-admin's src/utils/indicatorAlerts.ts — same Supertrend/
MACD/MA Cross/RSI/Stochastic math and the same "compare only the last two
closed bars" condition check, so alert_checker.py's persistent backend
evaluation agrees with Chart.tsx's client-side one on when an indicator-
condition alert (created via the Settings tab's "Technical" alert type) has
actually fired. Every alert has a base resolution (indicatorResolution),
locked to whichever chart resolution was active when it was created — but
each condition in the chain (the primary, and any additionalConditions row)
may instead carry its own explicit "resolution" override, letting one alert
mix e.g. an RSI checked on 15m with an EMA checked on 30m (see
alert_checker.py's _effective_entry_resolution). A condition with no
override just falls back to the alert's own base resolution.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

DAY_SECONDS = 86400

_IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN_MINUTES = 9 * 60 + 15  # 09:15 IST
MARKET_CLOSE_MINUTES = 15 * 60 + 30  # 15:30 IST
BAR_CLOSE_BUFFER_SECONDS = 5.0  # grace period for the broker's historical API to have the just-closed bar

Bar = dict


def _closes(bars: list[Bar]) -> list[float]:
    return [float(b["close"]) for b in bars]


def _rolling_average(values: list[float | None], length: int) -> list[float | None]:
    n = len(values)
    out: list[float | None] = [None] * n
    for i in range(length - 1, n):
        window = values[i - length + 1 : i + 1]
        if any(v is None for v in window):
            continue
        out[i] = sum(window) / length
    return out


def _ema(values: list[float], length: int) -> list[float | None]:
    n = len(values)
    out: list[float | None] = [None] * n
    if n < length:
        return out
    prev = sum(values[:length]) / length
    out[length - 1] = prev
    k = 2 / (length + 1)
    for i in range(length, n):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def compute_supertrend_trend(bars: list[Bar], length: int = 10, factor: float = 3.0) -> list[int]:
    n = len(bars)
    if n == 0:
        return []

    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    closes = _closes(bars)

    tr = [0.0] * n
    for i in range(n):
        tr[i] = (
            highs[i] - lows[i]
            if i == 0
            else max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        )

    atr = [0.0] * n
    for i in range(n):
        atr[i] = sum(tr[: i + 1]) / (i + 1) if i < length else (atr[i - 1] * (length - 1) + tr[i]) / length

    up = [0.0] * n
    dn = [0.0] * n
    trend = [1] * n
    for i in range(n):
        hl2 = (highs[i] + lows[i]) / 2
        basic_up = hl2 - factor * atr[i]
        basic_dn = hl2 + factor * atr[i]

        if i == 0:
            up[i], dn[i], trend[i] = basic_up, basic_dn, 1
            continue

        up[i] = max(basic_up, up[i - 1]) if closes[i - 1] > up[i - 1] else basic_up
        dn[i] = min(basic_dn, dn[i - 1]) if closes[i - 1] < dn[i - 1] else basic_dn

        if trend[i - 1] == -1 and closes[i] > dn[i - 1]:
            trend[i] = 1
        elif trend[i - 1] == 1 and closes[i] < up[i - 1]:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]

    return trend


def compute_macd(
    bars: list[Bar], fast_length: int = 12, slow_length: int = 26, signal_length: int = 9
) -> tuple[list[float | None], list[float | None]]:
    closes = _closes(bars)
    fast = _ema(closes, fast_length)
    slow = _ema(closes, slow_length)
    n = len(closes)

    macd_line: list[float | None] = [None] * n
    for i in range(n):
        if fast[i] is not None and slow[i] is not None:
            macd_line[i] = fast[i] - slow[i]

    first_valid = next((i for i, v in enumerate(macd_line) if v is not None), None)
    signal_line: list[float | None] = [None] * n
    if first_valid is not None:
        tail = [float(v) for v in macd_line[first_valid:]]  # type: ignore[arg-type]
        for offset, value in enumerate(_ema(tail, signal_length)):
            signal_line[first_valid + offset] = value

    return macd_line, signal_line


def compute_ma_cross(
    bars: list[Bar], short_length: int = 9, long_length: int = 21
) -> tuple[list[float | None], list[float | None]]:
    closes: list[float | None] = list(_closes(bars))
    return _rolling_average(closes, short_length), _rolling_average(closes, long_length)


def compute_rsi(bars: list[Bar], length: int = 14) -> list[float | None]:
    n = len(bars)
    out: list[float | None] = [None] * n
    if n < length + 1:
        return out
    closes = _closes(bars)

    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(1, length + 1):
        change = closes[i] - closes[i - 1]
        avg_gain += max(change, 0.0)
        avg_loss += max(-change, 0.0)
    avg_gain /= length
    avg_loss /= length
    out[length] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)

    for i in range(length + 1, n):
        change = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (length - 1) + max(change, 0.0)) / length
        avg_loss = (avg_loss * (length - 1) + max(-change, 0.0)) / length
        out[i] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)

    return out


def compute_stochastic(
    bars: list[Bar], k_length: int = 14, k_smoothing: int = 1, d_smoothing: int = 3
) -> tuple[list[float | None], list[float | None]]:
    n = len(bars)
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    closes = _closes(bars)

    raw_k: list[float | None] = [None] * n
    for i in range(k_length - 1, n):
        window_high = max(highs[i - k_length + 1 : i + 1])
        window_low = min(lows[i - k_length + 1 : i + 1])
        raw_k[i] = 0.0 if window_high == window_low else (closes[i] - window_low) / (window_high - window_low) * 100

    k = _rolling_average(raw_k, k_smoothing) if k_smoothing > 1 else raw_k
    d = _rolling_average(k, d_smoothing)
    return k, d


def evaluate_indicator_condition(
    indicator: str, condition: str, bars: list[Bar], value: float | None = None
) -> tuple[bool, float] | None:
    """Mirrors evaluateIndicatorCondition in indicatorAlerts.ts. Returns
    (matched, bar_time_ms) for the transition between the last two closed
    bars, or None if `indicator`/`condition` isn't recognized. `value`
    overrides the fixed threshold for a condition that has one (currently
    just RSI's Overbought/Oversold) — falls back to that condition's own
    default (70/30) when not given, for alerts saved before this existed."""
    n = len(bars)
    if n < 2:
        return None
    last, prev = n - 1, n - 2
    bar_time = float(bars[last]["time"])

    if indicator == "supertrend":
        trend = compute_supertrend_trend(bars, 10, 3.0)
        flipped_up = trend[prev] == -1 and trend[last] == 1
        flipped_down = trend[prev] == 1 and trend[last] == -1
        if condition == "downtrend_to_uptrend":
            return flipped_up, bar_time
        if condition == "uptrend_to_downtrend":
            return flipped_down, bar_time
        if condition == "trend_change":
            return (flipped_up or flipped_down), bar_time
        return None

    if indicator == "macd":
        macd_line, signal_line = compute_macd(bars)
        values = (macd_line[prev], signal_line[prev], macd_line[last], signal_line[last])
        if None in values:
            return False, bar_time
        m_prev, s_prev, m_last, s_last = values
        if condition == "macd_crosses_above_signal":
            return (m_prev <= s_prev and m_last > s_last), bar_time
        if condition == "macd_crosses_below_signal":
            return (m_prev >= s_prev and m_last < s_last), bar_time
        return None

    if indicator == "ma_cross":
        short, long_ = compute_ma_cross(bars)
        values = (short[prev], long_[prev], short[last], long_[last])
        if None in values:
            return False, bar_time
        sh_prev, lg_prev, sh_last, lg_last = values
        if condition == "short_crosses_above_long":
            return (sh_prev <= lg_prev and sh_last > lg_last), bar_time
        if condition == "short_crosses_below_long":
            return (sh_prev >= lg_prev and sh_last < lg_last), bar_time
        return None

    if indicator == "rsi":
        rsi = compute_rsi(bars, 14)
        values = (rsi[prev], rsi[last])
        if None in values:
            return False, bar_time
        r_prev, r_last = values
        # "_70"/"_30" are the pre-rename fixed Overbought/Oversold condition
        # keys — kept working exactly as before (same math) so an alert
        # saved before the Quantman-style "Crosses/Crossing Above/Below"
        # rename doesn't silently stop firing; indicatorAlerts.ts's catalog
        # only ever offers the new keys going forward.
        if condition in ("crosses_above", "crosses_above_70"):
            threshold = value if value is not None else 70
            return (r_prev <= threshold and r_last > threshold), bar_time
        if condition in ("crosses_below", "crosses_below_30"):
            threshold = value if value is not None else 30
            return (r_prev >= threshold and r_last < threshold), bar_time
        if condition == "crossing_above":
            threshold = value if value is not None else 70
            return (r_last > threshold), bar_time
        if condition == "crossing_below":
            threshold = value if value is not None else 30
            return (r_last < threshold), bar_time
        return None

    if indicator == "stochastic":
        k, d = compute_stochastic(bars)
        values = (k[prev], d[prev], k[last], d[last])
        if None in values:
            return False, bar_time
        k_prev, d_prev, k_last, d_last = values
        if condition == "k_crosses_above_d":
            return (k_prev <= d_prev and k_last > d_last), bar_time
        if condition == "k_crosses_below_d":
            return (k_prev >= d_prev and k_last < d_last), bar_time
        return None

    return None


def evaluate_price_condition(direction: str, value: float, bars: list[Bar]) -> tuple[bool, float] | None:
    """Mirrors evaluatePriceCondition in indicatorAlerts.ts. For a plain
    price-level condition mixed into the same AND chain as an indicator
    condition — the chain as a whole is bar-close-scheduled (driven by
    whichever resolution the indicator condition(s) locked), so the price
    leg has to be evaluated against bars too instead of the live-tick engine
    it would normally use standalone."""
    n = len(bars)
    if n < 2:
        return None
    last, prev = n - 1, n - 2
    bar_time = float(bars[last]["time"])
    prev_close = float(bars[prev]["close"])
    curr_close = float(bars[last]["close"])

    crossed_above = prev_close < value and curr_close >= value
    crossed_below = prev_close > value and curr_close <= value

    if direction == "crosses_above":
        return crossed_above, bar_time
    if direction == "crosses_below":
        return crossed_below, bar_time
    if direction == "greater_than":
        return curr_close > value, bar_time
    if direction == "less_than":
        return curr_close < value, bar_time
    return (crossed_above or crossed_below), bar_time


def get_indicator_lookback_seconds(resolution: str) -> int:
    """Mirrors getIndicatorLookbackMs in indicatorAlerts.ts (seconds here —
    the backend's bar-fetch takes from_ts/to_ts in seconds, not ms)."""
    if resolution == "1D":
        return DAY_SECONDS * 200
    if resolution == "3D":
        return DAY_SECONDS * 200 * 3
    if resolution == "1W":
        return DAY_SECONDS * 365 * 4
    if resolution == "1M":
        return DAY_SECONDS * 365 * 10
    try:
        minutes = float(resolution)
    except (TypeError, ValueError):
        return DAY_SECONDS * 30
    if minutes <= 0:
        return DAY_SECONDS * 30
    bars_per_trading_day = max(1, int((6.25 * 60) // minutes))
    days_needed = -(-150 // bars_per_trading_day) + 3  # ceil(150 / bars_per_day) + buffer
    return DAY_SECONDS * days_needed


def seconds_until_next_bar_close(resolution: str, now_ts: float) -> float:
    """Seconds from now_ts until `resolution`'s next bar closes (plus
    BAR_CLOSE_BUFFER_SECONDS so the broker's historical API has had a moment
    to make the just-closed bar available), assuming IST market hours
    (09:15-15:30). Intraday resolutions roll to the next periodic boundary;
    "1D"/"1W"/"1M" roll to the next 15:30 IST close. If today's relevant
    boundary already passed, rolls to tomorrow's.

    Correctness doesn't depend on knowing market holidays — a wake-up on a
    non-trading day simply finds no new bar and no-ops (alert_checker.py's
    scheduler loop clamps its own sleep separately so a freshly created
    alert on an idle resolution isn't stranded waiting on this in the
    meantime)."""
    now = datetime.fromtimestamp(now_ts, tz=_IST)
    now_minutes = now.hour * 60 + now.minute + now.second / 60.0

    if resolution in ("1D", "3D", "1W", "1M"):
        if now_minutes < MARKET_CLOSE_MINUTES:
            target_minutes, days_ahead = float(MARKET_CLOSE_MINUTES), 0
        else:
            target_minutes, days_ahead = float(MARKET_CLOSE_MINUTES), 1
    else:
        try:
            interval = float(resolution)
        except (TypeError, ValueError):
            interval = 5.0
        if interval <= 0:
            interval = 5.0

        if now_minutes < MARKET_OPEN_MINUTES:
            target_minutes, days_ahead = MARKET_OPEN_MINUTES + interval, 0
        elif now_minutes < MARKET_CLOSE_MINUTES:
            steps = int((now_minutes - MARKET_OPEN_MINUTES) // interval) + 1
            candidate = MARKET_OPEN_MINUTES + steps * interval
            if candidate <= MARKET_CLOSE_MINUTES:
                target_minutes, days_ahead = candidate, 0
            else:
                target_minutes, days_ahead = float(MARKET_CLOSE_MINUTES), 0
        else:
            target_minutes, days_ahead = MARKET_OPEN_MINUTES + interval, 1

    seconds_until_target = (target_minutes - now_minutes) * 60.0 + days_ahead * DAY_SECONDS
    return seconds_until_target + BAR_CLOSE_BUFFER_SECONDS
