import argparse
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Start mcp-spark-ui")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    # Databricks Apps assigns the listen port via DATABRICKS_APP_PORT.
    port = int(os.environ.get("DATABRICKS_APP_PORT", args.port))
    uvicorn.run(
        "server.app:combined_app",
        host="0.0.0.0",
        port=port,
    )
