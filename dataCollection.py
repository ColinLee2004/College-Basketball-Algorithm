import json
import pandas as pd
import numpy as np
import time
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

fn = Path("cbb_kenpom") / "data" / "cbb_raw.csv"
BASE = "https://ncaa-api.henrygd.me"

def make_session():
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def scoreboard_path(d):
    return f"/scoreboard/basketball-men/d1/{d:%Y/%m/%d}/all-conf"

def extract_game_id(game_url):
    if not game_url or "/game/" not in game_url:
        return None
    return game_url.split("/game/")[-1].split("/")[0]

def build_game_date_map(start, end, session):
    # store gid -> known date so you don't call /game/{id} later
    gid_to_date = {}
    d = start
    while d <= end:
        sb = session.get(BASE + scoreboard_path(d), timeout=(5, 20)).json()
        for g in sb.get("games", []):
            gid = extract_game_id(g.get("game", {}).get("url"))
            if gid:
                gid_to_date[gid] = d
        d += timedelta(days=1)
    return gid_to_date

def load_data(gid_to_date, session, checkpoint_every=200, checkpoint_path="data/rows_checkpoint.jsonl"):

    rows = []
    ids = sorted(gid_to_date.keys(), key=lambda g: gid_to_date[g])
    today = date.today()

    for i, gid in enumerate(ids, 1):
        game_date = gid_to_date[gid]
        if game_date > today:
            break

        try:
            r = session.get(f"{BASE}/game/{gid}/team-stats", timeout=(3, 8))
            r.raise_for_status()
            team_data = r.json()
        except Exception:
            continue

        teams = team_data.get("teams", [])
        box = team_data.get("teamBoxscore", [])
        if len(teams) != 2 or len(box) != 2:
            continue

        team_id_to_name = {int(t["teamId"]): t["nameShort"] for t in teams}
        team_id_to_home = {int(t["teamId"]): t["isHome"] for t in teams}
        game_date = gid_to_date[gid]


        for b in box:
            tid = int(b["teamId"])
            opp_id = next(k for k in team_id_to_name if k != tid)
            stats = b["teamStats"]

            fga = int(stats["fieldGoalsAttempted"]) or 1
            fta = int(stats["freeThrowsAttempted"]) or 1
            tpa = int(stats["threePointsAttempted"]) or 1

            rows.append({
                "teamID": tid,
                "teamName": team_id_to_name[tid],
                "isHome": team_id_to_home[tid],
                "oppTeamID": opp_id,
                "oppTeamName": team_id_to_name[opp_id],
                "fieldGoalsMade": int(stats["fieldGoalsMade"]),
                "fieldGoalsAttempted": int(stats["fieldGoalsAttempted"]),
                "fieldGoalPercentage": round(int(stats["fieldGoalsMade"]) / fga * 100, 1),
                "freeThrowsMade": int(stats["freeThrowsMade"]),
                "freeThrowsAttempted": int(stats["freeThrowsAttempted"]),
                "freeThrowPercentage": round(int(stats["freeThrowsMade"]) / fta * 100, 1),
                "threePointsMade": int(stats["threePointsMade"]),
                "threePointsAttempted": int(stats["threePointsAttempted"]),
                "threePointPercentage": round(int(stats["threePointsMade"]) / tpa * 100, 1),
                "offensiveRebounds": int(stats["offensiveRebounds"]),
                "totalRebounds": int(stats["totalRebounds"]),
                "assists": int(stats["assists"]),
                "turnovers": int(stats["turnovers"]),
                "personalFouls": int(stats["personalFouls"]),
                "steals": int(stats["steals"]),
                "blockedShots": int(stats["blockedShots"]),
                "gameId": gid,
                "date": str(game_date),
            })

            print(team_id_to_name[tid], 'vs', team_id_to_name[opp_id], 'on', game_date)

        time.sleep(0.05)  # gentle throttle to reduce server strain

    return rows

def update_cbb_raw(fn):

    cur_raw_df = pd.read_csv(fn)
    max_dt = datetime.strptime(cur_raw_df['date'].max(), "%Y-%m-%d")

    start = max_dt.date() + timedelta(days=1)
    end = date.today() - timedelta(days=1)
    print(start)
    print(end)

    session = make_session()
    gid_to_date = build_game_date_map(start= start, end = end, session = session)
    rows = load_data(gid_to_date, session)
    only_new_df = pd.DataFrame(rows)

    updated_cbb_raw = pd.concat([cur_raw_df, only_new_df])
    updated_cbb_raw.to_csv(fn, index=False)
    return updated_cbb_raw


update_cbb_raw(fn)