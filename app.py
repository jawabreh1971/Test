
import os, json, sqlite3
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

API_KEY = os.getenv("NIKOLA_API_KEY", "your-local-key")
DB_PATH = os.getenv("DB_PATH", "app.db")

with open(os.path.join(os.path.dirname(__file__), "schema.json"), "r", encoding="utf-8") as f:
    SCHEMA = json.load(f)

app = FastAPI(title=SCHEMA.get("name", "App"))
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

def auth(x_api_key: str | None):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-KEY")

def get_conn():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; return conn

def init_db():
    conn = get_conn(); cur = conn.cursor()
    for res in SCHEMA["resources"]:
        cols = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for name, col in res["fields"].items():
            typ = col.get("type", "text")
            if typ == "number": cols.append(f"{name} REAL")
            elif typ == "integer": cols.append(f"{name} INTEGER")
            else: cols.append(f"{name} TEXT")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {res['name']} ({', '.join(cols)})")
    conn.commit(); conn.close()

init_db()

@app.get("/healthz")
def healthz():
    return {"ok": True, "app": SCHEMA.get("name")}

@app.get("/api/{resource}")
def list_items(resource: str, x_api_key: str | None = Header(None, convert_underscores=False)):
    auth(x_api_key)
    names = [r["name"] for r in SCHEMA["resources"]]
    if resource not in names: raise HTTPException(404, "Resource not found")
    conn = get_conn(); rows = conn.execute(f"SELECT * FROM {resource}").fetchall(); conn.close()
    return [dict(r) for r in rows]

@app.post("/api/{resource}")
def create_item(resource: str, payload: dict, x_api_key: str | None = Header(None, convert_underscores=False)):
    auth(x_api_key)
    res = next((r for r in SCHEMA["resources"] if r["name"] == resource), None)
    if not res: raise HTTPException(404, "Resource not found")
    fields = list(res["fields"].keys()); data = payload.get("data", {})
    values = [data.get(k) for k in fields]
    conn = get_conn(); cur = conn.cursor()
    cur.execute(f"INSERT INTO {resource} ({', '.join(fields)}) VALUES ({','.join(['?']*len(fields))})", values)
    conn.commit(); rid = cur.lastrowid
    row = conn.execute(f"SELECT * FROM {resource} WHERE id=?", (rid,)).fetchone(); conn.close()
    return dict(row)
