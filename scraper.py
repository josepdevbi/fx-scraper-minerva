import os
import re
import requests
from datetime import date
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

URL = "https://30rates.com/usd-cop"

def scrape_trm():
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
    soup = BeautifulSoup(html, "html.parser")

    today_str   = date.today().isoformat()
    current_year = date.today().year
    records = []

    # ── Tasa actual del día ───────────────────────────────────────────────────
    m = re.search(
        r"exchange rate equals\s+([\d,]+\.?\d*)\s+Colombian Pesos",
        html, re.IGNORECASE,
    )
    if not m:
        raise ValueError("No se pudo extraer la TRM actual del HTML.")

    current_rate = int(float(m.group(1).replace(",", "")))
    print(f"TRM actual: {current_rate}")

    records.append({
        "fecha_captura":    today_str,
        "fecha_pronostico": today_str,
        "dia_semana":       date.today().strftime("%A"),
        "min_cop":          current_rate,
        "max_cop":          current_rate,
        "rate_cop":         current_rate,
        "tipo":             "actual",
    })

    # ── Pronósticos de la tabla ───────────────────────────────────────────────
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows[1:]:  # saltar encabezado
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) < 5:
                continue
            try:
                # cols: Date(MM/DD), Weekday, Min, Max, Rate
                month, day = cols[0].split("/")
                fecha_pronostico = date(current_year, int(month), int(day)).isoformat()
                weekday  = cols[1]
                min_cop  = int(cols[2].replace(",", ""))
                max_cop  = int(cols[3].replace(",", ""))
                rate_cop = int(cols[4].replace(",", ""))

                records.append({
                    "fecha_captura":    today_str,
                    "fecha_pronostico": fecha_pronostico,
                    "dia_semana":       weekday,
                    "min_cop":          min_cop,
                    "max_cop":          max_cop,
                    "rate_cop":         rate_cop,
                    "tipo":             "pronostico",
                })
            except Exception:
                continue

    print(f"Total registros: {len(records)} (1 actual + {len(records)-1} pronósticos)")
    return records


def upsert_records(records: list):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    result = (
        supabase.table("usd_cop_30rates")
        .upsert(records, on_conflict="fecha_captura,fecha_pronostico")
        .execute()
    )
    print(f"Supabase upsert OK → {len(records)} registros")
    return result


if __name__ == "__main__":
    try:
        records = scrape_trm()
        upsert_records(records)
    except Exception as e:
        print(f"Error en scraping: {e}")
        raise
