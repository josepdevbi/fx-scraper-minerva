import os
import re
import requests
from datetime import date
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

URL = "https://30rates.com/usd-cop"

def scrape_trm() -> float:
    print(f"Iniciando scrape: {date.today()}")
    print(f"Fetching {URL}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        )
    }
    resp = requests.get(URL, headers=headers, timeout=15)
    resp.raise_for_status()
    html = resp.text

    # ── Estrategia 1: párrafo introductorio ──────────────────────────────────
    # "Current USD to COP exchange rate equals 3547.86 Colombian Pesos"
    m = re.search(
        r"exchange rate equals\s+([\d,]+\.?\d*)\s+Colombian Pesos",
        html,
        re.IGNORECASE,
    )
    if m:
        return float(m.group(1).replace(",", ""))

    # ── Estrategia 2: fallback — primer número grande en el bloque "TODAY" ───
    m = re.search(
        r"USD TO COP TODAY.*?(\d{4,5}\.\d{2})",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        return float(m.group(1).replace(",", ""))

    raise ValueError("No se pudo extraer la TRM del HTML. Revisar estructura del sitio.")


def upsert_trm(rate: float):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    today = date.today().isoformat()
    data = {"date": today, "usd_cop": rate, "source": "30rates"}
    result = (
        supabase.table("trm_usd_cop")
        .upsert(data, on_conflict="date")
        .execute()
    )
    print(f"Supabase upsert OK → {today}: {rate}")
    return result


if __name__ == "__main__":
    try:
        rate = scrape_trm()
        print(f"TRM extraída: {rate}")
        upsert_trm(rate)
    except Exception as e:
        print(f"Error en scraping: {e}")
        raise  # exit code 1 → falla el job
