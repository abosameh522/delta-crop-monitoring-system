from __future__ import annotations
import json
from datetime import date
from pathlib import Path

def append_run_log(root: Path, record: dict) -> None:
    path=root/'_logs'/'pipeline_runs.jsonl'; path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a',encoding='utf-8') as f: f.write(json.dumps(record,sort_keys=True,default=str)+'\n')

def is_complete_season(year:int, season_end:date, as_of:date, weather_dates:set[date], start:date)->bool:
    return as_of >= season_end and len(weather_dates)==(season_end-start).days+1

def week_status(week_start:date, as_of:date)->str:
    return 'evaluated' if (week_start).toordinal()+6 <= as_of.toordinal() else 'not_yet_observed'
