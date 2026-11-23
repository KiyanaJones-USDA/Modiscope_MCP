
import io, time, threading, requests, pandas as pd

# Official public sources (tiny XLSX files):
NAICS_XLSX_URL = "https://www.census.gov/naics/reference_files_tools/2022_NAICS_Structure.xlsx"  # Census NAICS portal 
PSC_XLSX_URL   = "https://www.acquisition.gov/sites/default/files/manual/PSC%20Manual%20April%202025.xlsx"  # Acquisition.gov PSC Manual (XLSX) 

TTL_SECONDS = 24 * 3600

_state = {
    "naics": {"loaded_at": 0, "by_code": {}, "by_text": []},
    "psc":   {"loaded_at": 0, "by_code": {}, "by_text": []}
}
_lock = threading.Lock()

def _fresh(cache_key):
    return (time.time() - _state[cache_key]["loaded_at"]) < TTL_SECONDS and _state[cache_key]["by_code"]

def load_naics(force=False):
    with _lock:
        if not force and _fresh("naics"):
            return _state["naics"]
        r = requests.get(NAICS_XLSX_URL, timeout=60)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content)).rename(columns={
            "2022 NAICS Code": "code",
            "2022 NAICS Title": "title"
        })
        df["code"] = df["code"].astype(str)
        df["sector"]    = df["code"].str[:2]
        df["subsector"] = df["code"].str[:3]
        df["group"]     = df["code"].str[:4]

        by_code, by_text = {}, []
        for _, row in df.iterrows():
            entry = {
                "title": row["title"],
                "sector": row["sector"],
                "subsector": row["subsector"],
                "industry_group": row["group"],
                "source_url": "https://www.census.gov/naics/"  # official portal 
            }
            by_code[row["code"]] = entry
            by_text.append((row["code"], row["title"].lower()))
        _state["naics"] = {"loaded_at": time.time(), "by_code": by_code, "by_text": by_text}
        return _state["naics"]

def load_psc(force=False):
    with _lock:
        if not force and _fresh("psc"):
            return _state["psc"]
        r = requests.get(PSC_XLSX_URL, timeout=60)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content))
        # Normalize common PSC columns
        rename = {}
        for c in df.columns:
            lc = c.lower()
            if c.strip() == "PSC": rename[c] = "code"
            elif "psc name" in lc: rename[c] = "name"
            elif "psc family" in lc or lc == "family": rename[c] = "family"
            elif "psc description" in lc: rename[c] = "description"
        df = df.rename(columns=rename)
        df["code"] = df["code"].astype(str).str.upper().str.strip()

        by_code, by_text = {}, []
        for _, row in df.iterrows():
            code = row.get("code")
            if not code: continue
            entry = {
                "name": (row.get("name") or "").strip(),
                "family": (row.get("family") or "").strip(),
                "description": (row.get("description") or "").strip(),
                "source_url": "https://www.acquisition.gov/psc-manual"  # official PSC portal 
            }
            by_code[code] = entry
            by_text.append((code, entry["name"].lower() + " " + entry["description"].lower()))
        _state["psc"] = {"loaded_at": time.time(), "by_code": by_code, "by_text": by_text}
        return _state["psc"]
