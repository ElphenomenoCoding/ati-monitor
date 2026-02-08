"""
ATI Load Monitor — Streamlined Backend
FastAPI + Telegram notifications for ATI.SU freight monitoring.
"""

import asyncio, json, logging, time, uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import os
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ati")

# === Config ===
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ATI_SEARCH = "https://loads.ati.su/webapi/public/v1.0/loads/search"
ATI_SUGGEST = "https://loads.ati.su/gw/gis-dict/public/v1/autocomplete/suggestions"
ATI_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://loads.ati.su",
    "referer": "https://loads.ati.su/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


# === Models ===
class GeoLocation(BaseModel):
    id: Optional[int] = None
    type: Optional[int] = None
    name: Optional[str] = None
    address: Optional[str] = None


class AlarmConfig(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "Новый будильник"
    active: bool = False
    telegram: str = ""
    from_location: Optional[GeoLocation] = None
    to_location: Optional[GeoLocation] = None
    weight_min: Optional[float] = None
    weight_max: Optional[float] = None
    volume_min: Optional[float] = None
    volume_max: Optional[float] = None
    date_option: str = "today"
    date_from: Optional[str] = None
    body_types: list[str] = []
    cargo_types: list[str] = []
    loading_types: list[str] = []
    payment_options: list[str] = []
    extra_options: list[str] = []
    length_min: Optional[float] = None
    length_max: Optional[float] = None
    width_min: Optional[float] = None
    width_max: Optional[float] = None
    height_min: Optional[float] = None
    height_max: Optional[float] = None
    pallets: Optional[int] = None
    ellipse_search: bool = False
    route_length: bool = False
    route_length_min: Optional[float] = None
    route_length_max: Optional[float] = None
    interval_seconds: int = 10


# === State ===
alarms: dict[str, AlarmConfig] = {}
alarm_tasks: dict[str, asyncio.Task] = {}
known_loads: dict[str, set] = {}
chat_id_cache: dict[str, int] = {}
ws_clients: list[WebSocket] = []
bot_users: dict[str, dict] = {}  # username -> {username, first_name, chat_id, last_seen}


# === Telegram ===
async def refresh_bot_users():
    """Scan getUpdates to build a list of all users who interacted with the bot."""
    if not BOT_TOKEN:
        return
    async with httpx.AsyncClient() as c:
        try:
            resp = await c.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                               params={"limit": 100, "offset": -100}, timeout=10)
            data = resp.json()
            if not data.get("ok"):
                return
            for upd in data.get("result", []):
                msg = upd.get("message", {})
                u = msg.get("from", {})
                uname = (u.get("username") or "").lower()
                if uname:
                    cid = msg.get("chat", {}).get("id")
                    bot_users[uname] = {
                        "username": uname,
                        "first_name": u.get("first_name", ""),
                        "chat_id": cid,
                        "last_seen": msg.get("date", 0),
                    }
                    if cid:
                        chat_id_cache[uname] = cid
        except Exception as e:
            log.error(f"refresh_bot_users error: {e}")


async def resolve_chat_id(username: str) -> Optional[int]:
    if not BOT_TOKEN:
        return None
    if username in chat_id_cache:
        return chat_id_cache[username]
    uname = username.lower().lstrip("@")
    await refresh_bot_users()
    return chat_id_cache.get(uname)


async def tg_send(chat_id: int, text: str):
    if not BOT_TOKEN:
        return
    async with httpx.AsyncClient() as c:
        try:
            await c.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                         json={"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                               "disable_web_page_preview": True}, timeout=10)
        except Exception as e:
            log.error(f"TG send error: {e}")


# === Payload Builder ===
def _minmax(lo, hi):
    d = {}
    if lo is not None: d["min"] = lo
    if hi is not None: d["max"] = hi
    return d or None


def _bitmask(values: list[str]) -> Optional[str]:
    if not values:
        return None
    try:
        combined = 0
        for v in values:
            combined |= int(v)
        return str(combined)
    except ValueError:
        return values[0] if values else None


def build_payload(a: AlarmConfig) -> dict:
    f = {}

    if a.from_location and a.from_location.id:
        f["from"] = {"id": a.from_location.id, "type": a.from_location.type or 2, "exact_only": True}
    if a.to_location and a.to_location.id:
        f["to"] = {"id": a.to_location.id, "type": a.to_location.type or 2, "exact_only": True}

    if a.date_option == "today":
        f["dates"] = {"date_option": "today"}
    elif a.date_option == "tomorrow":
        tmrw = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        f["dates"] = {"date_from": f"{tmrw}T00:00:00", "date_option": "manual"}
    elif a.date_option == "manual" and a.date_from:
        f["dates"] = {"date_from": f"{a.date_from}T00:00:00", "date_option": "manual"}

    for key, lo, hi in [
        ("weight", a.weight_min, a.weight_max), ("volume", a.volume_min, a.volume_max),
        ("length", a.length_min, a.length_max), ("width", a.width_min, a.width_max),
        ("height", a.height_min, a.height_max),
    ]:
        mm = _minmax(lo, hi)
        if mm:
            f[key] = mm

    if a.pallets is not None:
        f["pallets"] = a.pallets

    bt = _bitmask(a.body_types)
    if bt:
        f["truck_type"] = bt

    if a.cargo_types:
        try:
            f["cargo_types"] = [int(ct) for ct in a.cargo_types]
        except ValueError:
            pass

    lt = _bitmask(a.loading_types)
    if lt:
        f["loading_type"] = lt

    numeric_extras = 0
    special = {"excludeTenders", "teamDriving", "withAdr"}
    for opt in a.extra_options:
        if opt not in special:
            try:
                numeric_extras |= int(opt)
            except ValueError:
                pass

    for popt in a.payment_options:
        try:
            numeric_extras |= int(popt)
        except ValueError:
            pass

    if numeric_extras:
        f["extra_params"] = numeric_extras

    if "excludeTenders" in a.extra_options:
        f["exclude_tenders"] = True
    if "teamDriving" in a.extra_options:
        f["team_driving"] = {"type": "exclude"}
    if "withAdr" in a.extra_options:
        f["adr"] = {"type": "include", "ids": [1, 2, 3, 4, 5, 6, 7, 8, 9]}

    f["sorting_type"] = 2
    return {"exclude_geo_dicts": True, "page": 1, "items_per_page": 20, "filter": f}


# === Load Formatting (Russian) ===
def fmt_load(load: dict) -> str:
    loading = load.get("loading", {})
    unloading = load.get("unloading", {})
    fr = loading.get("location", {}).get("city", "?")
    to = unloading.get("location", {}).get("city", "?")
    dist = load.get("route", {}).get("distance", "?")
    cargo = load.get("load", {})
    w, v, ct = cargo.get("weight", "?"), cargo.get("volume", "?"), cargo.get("cargoType", "")

    fd = loading.get("firstDate", "")
    ds = ""
    if fd:
        try:
            ds = datetime.fromisoformat(fd.replace("Z", "+00:00")).strftime("%d.%m.%Y")
        except Exception:
            ds = fd[:10]

    lid = load.get("id", "")
    url = f"https://loads.ati.su/loadinfo/{lid}" if lid else ""

    rate = load.get("rate", {})
    price = rate.get("price")
    rt = f"💰 {price} {rate.get('currencyName', '')}" if price else "💰 Ставка скрыта"

    msg = (
        f"🚛 <b>{fr} → {to}</b>\n"
        f"📏 {dist} км\n"
        f"📅 {ds}\n"
        f"📦 {ct}\n"
        f"⚖️ {w} т | 📐 {v} м³\n"
        f"{rt}"
    )
    if url:
        msg += f'\n\n🔗 <a href="{url}">Открыть на ATI.SU</a>'
    return msg


# === WS Broadcast ===
async def broadcast(data: dict):
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.remove(ws)


# === Monitor Loop ===
async def monitor(alarm_id: str):
    log.info(f"Monitor started: {alarm_id}")
    known_loads.setdefault(alarm_id, set())
    chat_id = None
    errors = 0

    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            a = alarms.get(alarm_id)
            if not a or not a.active:
                break

            if chat_id is None and a.telegram:
                chat_id = await resolve_chat_id(a.telegram)

            interval = max(a.interval_seconds, 5)

            try:
                payload = build_payload(a)
                resp = await client.post(ATI_SEARCH, headers=ATI_HEADERS, json=payload)

                if resp.status_code != 200:
                    errors += 1
                    log.warning(f"[{a.name}] API {resp.status_code} (err #{errors})")
                    await broadcast({"type": "error", "alarm_id": alarm_id,
                                     "message": f"Ошибка API: {resp.status_code}"})
                    await asyncio.sleep(interval * min(errors, 6))
                    continue

                errors = 0
                data = resp.json()
                loads = data.get("loads", [])

                new = []
                for ld in loads:
                    lid = ld.get("id")
                    if lid and lid not in known_loads[alarm_id]:
                        known_loads[alarm_id].add(lid)
                        new.append(ld)

                await broadcast({"type": "status", "alarm_id": alarm_id,
                                 "total": data.get("total", len(loads)),
                                 "new_count": len(new),
                                 "known_count": len(known_loads[alarm_id]),
                                 "timestamp": datetime.now().isoformat()})

                for ld in new:
                    msg = fmt_load(ld)
                    await broadcast({"type": "new_load", "alarm_id": alarm_id,
                                     "alarm_name": a.name, "message": msg, "load_id": ld.get("id")})
                    if chat_id:
                        await tg_send(chat_id, f"🔔 <b>[{a.name}]</b>\n\n{msg}")
                        await asyncio.sleep(0.2)

                if new:
                    log.info(f"[{a.name}] {len(new)} new (tracking {len(known_loads[alarm_id])})")

            except httpx.TimeoutException:
                log.warning(f"[{a.name}] Timeout")
                await broadcast({"type": "error", "alarm_id": alarm_id, "message": "Таймаут запроса"})
            except Exception as e:
                log.error(f"[{a.name}] Error: {e}")
                await broadcast({"type": "error", "alarm_id": alarm_id, "message": str(e)})

            await asyncio.sleep(interval)

    log.info(f"Monitor stopped: {alarm_id}")


def start_task(aid: str):
    if aid in alarm_tasks:
        alarm_tasks[aid].cancel()
    alarm_tasks[aid] = asyncio.create_task(monitor(aid))


def stop_task(aid: str):
    t = alarm_tasks.pop(aid, None)
    if t:
        t.cancel()


# === FastAPI ===
app = FastAPI(title="ATI Load Monitor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC = Path(__file__).parent / "static"
if STATIC.exists():
    app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.on_event("startup")
async def on_startup():
    await refresh_bot_users()
    # Self-ping to prevent Render free tier from sleeping
    asyncio.create_task(keep_alive())


async def keep_alive():
    """Ping ourselves every 10 minutes to prevent Render free tier sleep."""
    import os
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        return  # Not on Render, skip
    await asyncio.sleep(60)  # Wait for startup
    async with httpx.AsyncClient() as c:
        while True:
            try:
                await c.get(f"{url}/api/bot-status", timeout=10)
                log.info("Keep-alive ping sent")
            except Exception:
                pass
            await asyncio.sleep(600)  # Every 10 minutes


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse((Path(__file__).parent / "templates" / "index.html").read_text(encoding="utf-8"))


@app.get("/api/alarms")
async def get_alarms():
    return {"alarms": [a.model_dump() for a in alarms.values()]}


@app.post("/api/alarms")
async def create_alarm(c: AlarmConfig):
    c.id = str(uuid.uuid4())
    alarms[c.id] = c
    return {"alarm": c.model_dump()}


@app.put("/api/alarms/{aid}")
async def update_alarm(aid: str, c: AlarmConfig):
    c.id = aid
    was_active = alarms.get(aid, AlarmConfig()).active
    alarms[aid] = c
    if c.active and not was_active:
        known_loads.setdefault(aid, set())
        start_task(aid)
    elif not c.active and was_active:
        stop_task(aid)
    return {"alarm": c.model_dump()}


@app.delete("/api/alarms/{aid}")
async def delete_alarm(aid: str):
    stop_task(aid)
    alarms.pop(aid, None)
    known_loads.pop(aid, None)
    return {"ok": True}


@app.post("/api/alarms/{aid}/start")
async def start_alarm(aid: str):
    a = alarms.get(aid)
    if not a:
        return {"error": "Not found"}
    a.active = True
    known_loads.setdefault(aid, set())
    start_task(aid)
    return {"ok": True}


@app.post("/api/alarms/{aid}/stop")
async def stop_alarm(aid: str):
    a = alarms.get(aid)
    if not a:
        return {"error": "Not found"}
    a.active = False
    stop_task(aid)
    return {"ok": True}


@app.post("/api/alarms/{aid}/reset")
async def reset_alarm(aid: str):
    known_loads[aid] = set()
    return {"ok": True}


@app.get("/api/bot-users")
async def get_bot_users():
    """Return all users who have sent /start to the bot."""
    await refresh_bot_users()
    return {"users": list(bot_users.values())}


@app.post("/api/suggest")
async def suggest(body: dict):
    async with httpx.AsyncClient(timeout=10) as c:
        resp = await c.post(ATI_SUGGEST, headers={"Content-Type": "application/json",
                            "Origin": "https://loads.ati.su", "Referer": "https://loads.ati.su/"},
                            json={"prefix": body.get("prefix", ""), "suggestion_types": 7, "limit": 10})
        return resp.json()


@app.post("/api/test-search")
async def test_search(c: AlarmConfig):
    payload = build_payload(c)
    log.info(f"Test search: {json.dumps(payload, ensure_ascii=False)}")
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(ATI_SEARCH, headers=ATI_HEADERS, json=payload)
        if resp.status_code != 200:
            return {"error": f"ATI API {resp.status_code}", "payload_sent": payload}
        data = resp.json()
        return {
            "total": data.get("total", 0),
            "loads": [{"id": l.get("id"), "message": fmt_load(l)} for l in data.get("loads", [])[:10]],
            "payload_sent": payload,
        }


@app.post("/api/debug-payload")
async def debug_payload(c: AlarmConfig):
    return {"payload": build_payload(c)}


@app.get("/api/bot-status")
async def bot_status():
    if not BOT_TOKEN:
        return {"configured": False, "message": "BOT_TOKEN не настроен"}
    async with httpx.AsyncClient(timeout=10) as c:
        try:
            resp = await c.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe")
            data = resp.json()
            if data.get("ok"):
                b = data["result"]
                return {"configured": True, "bot_name": b.get("first_name"),
                        "bot_username": b.get("username")}
            return {"configured": False, "message": "Неверный токен"}
        except Exception as e:
            return {"configured": False, "message": str(e)}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)


if __name__ == "__main__":
    import os, uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"\n{'='*50}")
    print(f"  ATI Load Monitor — http://localhost:{port}")
    print("=" * 50)
    if not BOT_TOKEN:
        print("  ⚠️  BOT_TOKEN не настроен!\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")