import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI()
received = []

@app.post("/ingest")
async def ingest(req: Request):
    body = await req.json()
    received.append(body)
    return JSONResponse({"status": "ok", "received_count": len(received)})

@app.get("/health")
async def health():
    return {"ok": True, "received": len(received)}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "9009"))
    uvicorn.run(app, host="0.0.0.0", port=port)
