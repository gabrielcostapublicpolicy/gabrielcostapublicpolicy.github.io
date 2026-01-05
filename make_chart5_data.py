# make_chart5_data.py
from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
CHARTS_DIR = DATA_DIR / "charts"

BF_FILE = CHARTS_DIR / "chart1_uf_total_2021.json"
SAN_FILE = DATA_DIR / "brazil_poverty_map.json"
EDU_FILE = DATA_DIR / "education_poverty.json"
UNEMP_FILE = DATA_DIR / "unemployment_cleaned.json"

OUT_FILE = CHARTS_DIR / "chart5_multidimensional_scatter.json"

UF_TO_STATE = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}

def _norm(s: str) -> str:
    """Normalize state names for robust joins (lowercase, strip accents, collapse spaces)."""
    s = s.strip().lower()
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    s = s.replace("-", " ")
    s = " ".join(s.split())
    return s

STATE_TO_UF = {_norm(v): k for k, v in UF_TO_STATE.items()}

def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def as_float(x: Any) -> Optional[float]:
    """
    Convert common messy inputs into float.
    Handles:
      - None / "" -> None
      - "8,4" -> 8.4
      - "83%" -> 83.0
      - " 12.3 " -> 12.3
    """
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            s = x.strip()
            if s == "":
                return None
            s = s.replace("%", "")
            s = s.replace(",", ".")
            return float(s)
        return float(x)
    except Exception:
        return None

# ---------------------------
# Loaders (all become mappings keyed by normalized state name)
# ---------------------------

def load_bf_totals_by_state_norm() -> Dict[str, Dict[str, Any]]:
    """
    Returns:
      key = normalized state name
      value = {"State": "Bahia", "UF": "BA", "bf_total": 123.0}
    """
    if not BF_FILE.exists():
        print(f"[ERROR] Missing Bolsa Familia file: {BF_FILE}")
        return {}

    rows = read_json(BF_FILE)
    out: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        if not isinstance(r, dict):
            continue

        uf = r.get("UF") or r.get("uf")
        if not isinstance(uf, str) or len(uf.strip()) != 2:
            continue
        uf = uf.strip().upper()

        total = None
        for k in ["total_pago_2021", "total_paid_2021", "total_paid", "total_pago", "total"]:
            if k in r:
                total = as_float(r.get(k))
                break
        if total is None:
            continue

        state = UF_TO_STATE.get(uf)
        if not state:
            continue

        key = _norm(state)
        out[key] = {"State": state, "UF": uf, "bf_total": float(total)}

    print(f"[INFO] Bolsa Familia source: {BF_FILE}")
    print(f"[INFO] Bolsa Familia states parsed: {len(out)}")
    print(f"[INFO] Bolsa Familia sample (first 5): {list(out.values())[:5]}")
    return out

def load_sanitation_2022_by_state_norm() -> Dict[str, float]:
    if not SAN_FILE.exists():
        print(f"[WARN] Missing sanitation file: {SAN_FILE}")
        return {}

    rows = read_json(SAN_FILE)
    if not isinstance(rows, list):
        print("[WARN] Sanitation file format not recognized (expected list).")
        return {}

    out: Dict[str, float] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        st = r.get("State")
        if not isinstance(st, str):
            continue
        val = as_float(r.get("Sanitation_Access"))
        if val is None:
            continue
        out[_norm(st)] = float(val)

    print(f"[INFO] Sanitation source: {SAN_FILE} | states: {len(out)}")
    return out

def load_education_latest_by_state_norm() -> Dict[str, float]:
    if not EDU_FILE.exists():
        print(f"[WARN] Missing education file: {EDU_FILE}")
        return {}

    rows = read_json(EDU_FILE)
    if not isinstance(rows, list):
        print("[WARN] Education file format not recognized (expected list).")
        return {}

    best: Dict[str, Tuple[int, float]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue

        st = r.get("State")
        if not isinstance(st, str):
            continue

        year = r.get("Year")
        try:
            y = int(year)
        except Exception:
            continue

        ys = as_float(r.get("Years_Schooling"))
        if ys is None:
            continue

        key = _norm(st)
        prev = best.get(key)
        if prev is None or y > prev[0]:
            best[key] = (y, float(ys))

    out = {k: v for k, (_, v) in best.items()}
    print(f"[INFO] Education source: {EDU_FILE} | states: {len(out)}")
    return out

def load_unemployment_2022_avg_by_state_norm() -> Dict[str, float]:
    if not UNEMP_FILE.exists():
        print(f"[WARN] Missing unemployment file: {UNEMP_FILE}")
        return {}

    rows = read_json(UNEMP_FILE)
    if not isinstance(rows, list):
        print("[WARN] Unemployment file format not recognized (expected list).")
        return {}

    # Prefer 2022; otherwise use latest year available per state
    by_state_year: Dict[str, Dict[int, List[float]]] = {}

    for r in rows:
        if not isinstance(r, dict):
            continue

        st = r.get("City")  # In your file, "City" contains the state name
        if not isinstance(st, str):
            continue

        q = r.get("Quarter")
        if not isinstance(q, str) or len(q) < 4 or not q[:4].isdigit():
            continue
        year = int(q[:4])

        rate = as_float(r.get("Unemployment_Rate"))
        if rate is None:
            continue

        key = _norm(st)
        by_state_year.setdefault(key, {}).setdefault(year, []).append(float(rate))

    out: Dict[str, float] = {}
    for st_key, years_map in by_state_year.items():
        if 2022 in years_map and years_map[2022]:
            out[st_key] = float(mean(years_map[2022]))
        else:
            latest_year = max(years_map.keys())
            out[st_key] = float(mean(years_map[latest_year]))

    print(f"[INFO] Unemployment source: {UNEMP_FILE} | states: {len(out)} (prefer 2022 else latest)")
    return out

# ---------------------------
# Build long rows (one row per state per dimension)
# ---------------------------

def build_long_rows() -> List[Dict[str, Any]]:
    bf = load_bf_totals_by_state_norm()
    if not bf:
        print("[ERROR] No Bolsa Familia totals parsed. Cannot build Chart 5 data.")
        return []

    sanitation = load_sanitation_2022_by_state_norm()
    education = load_education_latest_by_state_norm()
    unemployment = load_unemployment_2022_avg_by_state_norm()

    out: List[Dict[str, Any]] = []
    added = {"Sanitation": 0, "Education": 0, "Unemployment": 0}
    missing = {"Sanitation": 0, "Education": 0, "Unemployment": 0}

    for st_key, bf_row in bf.items():
        st_name = bf_row["State"]
        uf = bf_row["UF"]
        bf_total = bf_row["bf_total"]

        v = sanitation.get(st_key)
        if v is not None:
            out.append({
                "UF": uf,
                "State": st_name,
                "bf_total": bf_total,
                "dimension": "Sanitation access (%)",
                "value": v
            })
            added["Sanitation"] += 1
        else:
            missing["Sanitation"] += 1

        v = education.get(st_key)
        if v is not None:
            out.append({
                "UF": uf,
                "State": st_name,
                "bf_total": bf_total,
                "dimension": "Education (years)",
                "value": v
            })
            added["Education"] += 1
        else:
            missing["Education"] += 1

        v = unemployment.get(st_key)
        if v is not None:
            out.append({
                "UF": uf,
                "State": st_name,
                "bf_total": bf_total,
                "dimension": "Unemployment (%)",
                "value": v
            })
            added["Unemployment"] += 1
        else:
            missing["Unemployment"] += 1

    print(f"[INFO] Added rows: {added}")
    print(f"[INFO] Missing (states without that dimension): {missing}")
    print(f"[INFO] Total long rows: {len(out)}")
    return out

def main() -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    rows = build_long_rows()
    OUT_FILE.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Wrote {len(rows)} rows to {OUT_FILE}")
    print("[INFO] Example rows (first 5):")
    for r in rows[:5]:
        print("  ", r)

if __name__ == "__main__":
    main()
