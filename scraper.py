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

    # DEBUG: imprimir fragmento del HTML para diagnóstico
    idx = html.lower().find("exchange rate")
    if idx >= 0:
        print(f"[DEBUG] Fragmento HTML: {repr(html[idx:idx+200])}")
    else:
        print(f"[DEBUG] 'exchange rate' NO encontrado. Primeros 500 chars:")
        print(repr(html[:500]))

    soup = BeautifulSoup(html, "html.parser")
    today_str    = date.today().isoformat()
    current_year = date.today().year
    records      = []

    # ── Estrategia 1: texto plano (sin tags HTML intermedios) ─────────────────
    # "exchange rate equals 3547.86 Colombian"
    m = re.search(
        r"exchange rate equals\s+[\*\s]*([\d,]+\.?\d*)[\*\s]*\s+Colombian",
        html, re.IGNORECASE,
    )

    # ── Estrategia 2: BeautifulSoup — buscar <strong> dentro del párrafo ──────
    if not m:
        for tag in soup.find_all(["p", "div", "span", "h2", "h3"]):
            text = tag.get_text(" ", strip=True)
            m2 = re.search(r"exchange rate equals\s+([\d,]+\.?\d+)", text, re.IGNORECASE)
            if m2:
                m = m2
                break

    # ── Estrategia 3: cualquier número 4-digit en el bloque TODAY ─────────────
    if not m:
        m = re.search(
            r"USD TO COP TODAY.*?(\d{4,5}(?:\.\d{1,2})?)",
            html, re.IGNORECASE | re.DOTALL,
        )

    # ── Fallback: frankfurter API (EUR base, calculamos COP) ─────────────────
    if not m:
        print("WARNING: No se encontró TRM en 30rates. Usando fallback frankfurter.app")
        fb = requests.get(
            "https://api.frankfurter.app/latest?from=USD&to=COP", timeout=10
        ).json()
        current_rate = int(float(fb["rates"]["COP"]))
        print(f"TRM fallback (frankfurter): {current_rate}")
    else:
        group = m.group(1) if hasattr(m, "group") else m.group(1)
        current_rate = int(float(group.replace(",", "")))
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
        for row in rows[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) < 5:
                continue
            try:
                month, day = cols[0].split("/")
                fecha_pronostico = date(current_year, int(month), int(day)).isoformat()
                records.append({
                    "fecha_captura":    today_str,
                    "fecha_pronostico": fecha_pronostico,
                    "dia_semana":       cols[1],
                    "min_cop":          int(cols[2].replace(",", "")),
                    "max_cop":          int(cols[3].replace(",", "")),
                    "rate_cop":         int(cols[4].replace(",", "")),
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
