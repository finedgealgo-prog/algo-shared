from features.backtest_engine import run_backtest

def strategy_worker(args: dict):
    strategy_id_str = str((args or {}).get("strategy_id_str") or "")
    backtest_req = dict((args or {}).get("backtest_req") or {})
    progress_dict = args.get("progress_dict")
    
    def on_progress(completed: int, total: int, day: str):
        if progress_dict is not None:
            progress_dict[strategy_id_str] = {"completed": completed, "total": total, "day": day}

    try:
        result = run_backtest(backtest_req, on_progress=on_progress)
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "completed",
            "error": None,
            "results": result,
        }
    except Exception as exc:
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "error",
            "error": str(exc),
            "results": None,
        }
