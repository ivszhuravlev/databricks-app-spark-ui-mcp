import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastmcp import FastMCP

from server.tools import load_tools
from server.utils import header_store


def _cors_origins() -> list[str]:
    raw = (os.environ.get("SPARK_UI_CORS_ORIGINS") or "").strip()
    if raw:
        return [item.strip() for item in raw.split(",") if item.strip()]
    host = (os.environ.get("DATABRICKS_HOST") or "").strip()
    if not host:
        return []
    if not host.startswith("http"):
        host = "https://" + host
    return [host.rstrip("/")]

mcp_server = FastMCP(name="mcp-spark-ui")
load_tools(mcp_server)

# Stateless HTTP: clients may omit mcp-session-id.
mcp_app = mcp_server.http_app(path="/mcp", stateless_http=True)

app = FastAPI(
    title="mcp-spark-ui",
    description="Minimal Spark UI MCP",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {"status": "healthy", "mcp": "/mcp"}


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "healthy"}


combined_app = FastAPI(
    title="mcp-spark-ui",
    routes=[
        *mcp_app.routes,
        *app.routes,
    ],
    lifespan=mcp_app.lifespan,
)


@combined_app.middleware("http")
async def capture_headers(request: Request, call_next):
    token = request.headers.get("x-forwarded-access-token")
    header_store.set({"x-forwarded-access-token": token} if token else {})
    return await call_next(request)


# Last-added middleware is outermost (CORS preflight).
combined_app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
