# /// script
# requires-python = ">=3.11"
# dependencies = ["mcp>=1.0"]
# ///
"""
MCP server for barter-rs: exposes tools for strategy discovery,
market data inspection, and backtest execution.

Tools:
  - list_miraelis_strategies  → discover StrategyModule implementations
  - extract_market_data       → validate & preview market data files
  - run_strategy_backtest     → execute backtest with given parameters
"""
import json
import os
import subprocess
from pathlib import Path

from mcp.server import FastMCP

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIRAELIS_SRC = PROJECT_ROOT / "miraelis" / "src" / "strategy"
MARKET_DIR = PROJECT_ROOT / "bin" / "data" / "market"

mcp = FastMCP("mcp_barter_system")


# ── Tool: list_miraelis_strategies ────────────────────────────────────

@mcp.tool()
def list_miraelis_strategies() -> dict:
    """Scan miraelis source to discover active StrategyModule implementations
    and their parameter boundaries."""
    strategies = []
    for rs_file in sorted(MIRAELIS_SRC.glob("*.rs")):
        content = rs_file.read_text()
        if "impl StrategyModule for" not in content:
            continue

        name = rs_file.stem
        strategy_id = _extract_id(name)
        params = _extract_config_params(rs_file)

        strategies.append({
            "name": name,
            "id": strategy_id,
            "source_file": str(rs_file.relative_to(PROJECT_ROOT)),
            "parameters": params,
        })

    return {"strategies": strategies}


# ── Tool: extract_market_data ─────────────────────────────────────────

@mcp.tool()
def extract_market_data(file_name: str, market_dir: str | None = None) -> dict:
    """Validate a historical market data file and return metadata.

    Args:
        file_name: e.g. 'ALTUSDT.candle_1m'
        market_dir: optional override for data directory
    """
    directory = Path(market_dir) if market_dir else MARKET_DIR
    file_path = directory / file_name

    if not file_path.exists():
        available = sorted(
            [f.name for f in directory.iterdir() if f.is_file()]
        )[:30]
        return {
            "error": f"File not found: {file_path}",
            "market_dir": str(directory),
            "dir_exists": directory.exists(),
            "available_files": available,
        }

    stat = file_path.stat()
    with open(file_path) as f:
        raw_lines = [line.strip() for line in f if line.strip()]
        total_lines = len(raw_lines)
        head_lines = raw_lines[:5]

    # Detect format
    if head_lines and head_lines[0].startswith("{"):
        fmt = "jsonl"
        records = []
        for line in head_lines[:3]:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                records.append({"parse_error": line[:200]})
        columns = list(records[0].keys()) if records else []
        snapshot = records
    else:
        fmt = "text"
        columns = []
        snapshot = head_lines[:3]

    return {
        "file": str(file_path),
        "size_bytes": stat.st_size,
        "format": fmt,
        "columns": columns,
        "total_lines": total_lines,
        "snapshot": snapshot,
    }


# ── Tool: run_strategy_backtest ───────────────────────────────────────

@mcp.tool()
def run_strategy_backtest(
    strategy_name: str,
    data_file: str,
    parameters: dict,
) -> dict:
    """Execute a backtest for a miraelis strategy with given hyperparameters.

    Args:
        strategy_name: e.g. 'huge_momentum', 'frame', 'rocket'
        data_file: market data CSV/JSONL filename in bin/data/market/
        parameters: dict of strategy config overrides
    """
    param_json = json.dumps(parameters)

    # Run existing cargo test for this strategy module
    test_filter = f"strategy::{strategy_name}::tests::"

    cmd = [
        "cargo", "test", "-p", "miraelis-market-ingest",
        "--", test_filter,
        "--nocapture",
    ]

    env = os.environ.copy()
    env["BARTER_BACKTEST_PARAMS"] = param_json
    env["BARTER_BACKTEST_DATA"] = data_file

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )
        metrics = _parse_backtest_metrics(result.stdout)
        return {
            "success": result.returncode == 0,
            "exit_code": result.returncode,
            "strategy": strategy_name,
            "parameters": parameters,
            "metrics": metrics,
            "stderr_tail": (result.stderr or "")[-2000:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Backtest timed out after 180s"}
    except FileNotFoundError:
        return {"success": False, "error": "cargo not found — is Rust installed?"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


# ── Helpers ───────────────────────────────────────────────────────────

def _extract_id(module_name: str) -> int:
    """Map strategy module name → numeric id."""
    mapping = {
        "frame": 1,
        "rocket": 2,
        "huge_momentum": 6,
    }
    return mapping.get(module_name, 0)


def _extract_config_params(rs_file: Path) -> list[dict]:
    """Extract config struct fields from a strategy module source file."""
    content = rs_file.read_text()
    params = []
    in_struct = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("pub struct") and "Config" in stripped:
            in_struct = True
            continue
        if in_struct:
            if stripped == "}" or stripped.startswith("}"):
                break
            if stripped.startswith("pub ") and ":" in stripped:
                field = stripped.split(":")[0].replace("pub ", "").strip()
                ftype = stripped.split(":")[1].strip().rstrip(",")
                params.append({"name": field, "type": ftype})
    return params


def _parse_backtest_metrics(stdout: str) -> dict:
    """Extract quantitative metrics from cargo test output."""
    metrics = {}
    for line in stdout.splitlines():
        lower = line.lower().strip()
        for key in ["sharpe", "drawdown", "pnl", "return", "win_rate",
                     "profit_factor", "sortino"]:
            if key in lower:
                # Try key: value or key=value patterns
                for sep in [":", "=", " "]:
                    if f"{key}{sep}" in lower or f"{key} {sep}" in lower:
                        break
        # Fallback: try to find any number=value pattern
        if "=" in line and any(k in lower for k in
                               ["sharpe", "drawdown", "pnl", "return"]):
            try:
                parts = line.split()
                for p in parts:
                    if "=" in p and any(k in p.lower() for k in
                                        ["sharpe", "drawdown", "pnl", "return"]):
                        k, v = p.split("=", 1)
                        try:
                            metrics[k.strip()] = float(v.rstrip(","))
                        except ValueError:
                            metrics[k.strip()] = v.rstrip(",")
            except (ValueError, IndexError):
                pass
    return metrics


# ── Entry point ───────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
