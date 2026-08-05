from typing import Any

from fastapi import APIRouter, HTTPException

from scanner.service import _parse_ymd_date, get_index_historical_chart_bars

router = APIRouter(prefix="/common", tags=["common"])


@router.get("/historical-data/{instrument}/{frequency}/{startdate}/{enddate}")
async def common_historical_data(
    instrument: str,
    frequency: str,
    startdate: str,
    enddate: str,
) -> dict[str, Any]:
    """Historical OHLCV bars addressed by plain dates instead of unix
    timestamps — convenient for manual testing, e.g.
    /common/historical-data/nifty_50/1D/2025-01-01/2025-03-01
    """
    try:
        from_dt = _parse_ymd_date(startdate, field_name="startdate")
        to_dt = _parse_ymd_date(enddate, field_name="enddate")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        return get_index_historical_chart_bars(
            instrument,
            from_ts=int(from_dt.timestamp()),
            to_ts=int(to_dt.timestamp()),
            resolution=frequency,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
