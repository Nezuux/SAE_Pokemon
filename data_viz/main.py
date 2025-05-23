from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
import os
import psycopg

postgres_db=os.environ.get('POSTGRES_DB')
postgres_user=os.environ.get('POSTGRES_USER')
postgres_password=os.environ.get('POSTGRES_PASSWORD')
postgres_host=os.environ.get('POSTGRES_HOST')
postgres_port=os.environ.get('POSTGRES_PORT')

app = FastAPI()
app.mount("/front", StaticFiles(directory="front"), name="static")

@app.get("/")
async def root():
  return RedirectResponse("/front/index.html")

@app.get("/api/tournaments")
async def test_data():
  with psycopg.connect(f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}") as conn:
    with conn.cursor() as cur:
      cur.execute("select * from tournaments")
      return {"data": cur.fetchall()}
