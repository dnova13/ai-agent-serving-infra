from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import items

app = FastAPI(title="FastAPI Sample")

# React 등 다른 오리진에서 오는 요청을 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(items.router)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
