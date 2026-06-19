import os
import re
import requests
from datetime import date
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

URL = "https://30rates.com/usd-cop"

def fetch_html():
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
        "Referer": "https://www.google.com/",
    }
    resp = requests.get(URL, headers=headers, timeout=15)
    print(f"[DEBUG] Status code: {resp.status_code}")
    print(f"[DEBUG] Content-Length: {len(resp.text)}")
    print(f"[DEBUG] Primeros 300 chars: {repr(resp.text[:300])}")
    resp.raise_for_status()
    return resp.text


def get_rate_from_fallback_api() -> int:
    """Fallback robusto: exchangerate-api.com (open endpoint, sin key)."""
    apis = [
        "https://open.er-api.com/v6/latest/USD",
        "https://api.exchangerate-api.com/v4/latest/USD",
    ]
    for api_url in apis:
        try:
            r = requests.get(api_url, timeout=10)
            r.raise_for_status()
            data = r.json()
            rate = data.get("rates", {}).get("COP")
            if rate:
                print(f"TRM fallback ({api_url}): {rate}")
                return int(float(rate))
        except Exception as e:
            print(f"[DEBUG] Fallback {api_url} falló: {e}")
            continue
    raise ValueError("Todos los fallbacks de TRM fallaron.")


def scrape_trm():
    print(f"Iniciando scrape: {date.today()}")
    print(f"Fetching {URL}")

    today_str    = date.today().isoformat()
    current_year = date.today().year
    records      = []

    try:
        html = fetch_html()
    except Exception as e:
        print(f"[DEBUG] Error al obtener HTML: {e}")
        html = ""

    idx = html.lower().find("exchange rate equals")
    if idx >= 0:
        print(f"[DEBUG] Fragmento HTML: {repr(html[idx:idx+200])}")
    else:
        print("[DEBUG] 'exchange rate equals' NO encontrado en el HTML.")

    soup = BeautifulSoup(html, "html.parser") if html else None

    # ── Estrategia 1: regex sobre HTML crudo ──────────────────────────────────
    m = re.search(
        r"exchange rate equals\s+[\*\s]*([\d,]+\.?\d*)[\*\s]*\s+Colombian",
        html, re.IGNORECASE,
    ) if html else None

    # ── Estrategia 2: BeautifulSoup — texto limpio de cada tag ────────────────
    if not m and soup:
        for tag in soup.find_all(["p", "div", "span", "h2", "h3"]):
            text = tag.get_text(" ", strip=True)
            m2 = re.search(r"exchange rate equals\s+([\d,]+\.?\d+)", text, re.IGNORECASE)
            if m2:
                m = m2
                break

    # ── Estrategia 3: primer número 4-5 dígitos en bloque TODAY ───────────────
    if not m and html:
        m = re.search(
            r"USD TO COP TODAY.*?(\d{4,5}(?:\.\d{1,2})?)",
            html, re.IGNORECASE | re.DOTALL,
        )

    if m:
        current_rate = int(float(m.group(1).replace(",", "")))
        print(f"TRM actual (30rates): {current_rate}")
    else:
        print("WARNING: No se encontró TRM en 30rates. Usando fallback API.")
        current_rate = get_rate_from_fallback_api()

    # ── Registro de la tasa actual del día ────────────────────────────────────
    records.append({
        "fecha_captura":    today_str,
        "fecha_pronostico": today_str,
        "dia_semana":       date.today().strftime("%A"),
        "min_cop":          current_rate,
        "max_cop":          current_rate,
        "rate_cop":         current_rate,
        "tipo":             "historico",
    })

    # ── Pronósticos de la tabla (solo si tenemos HTML válido) ─────────────────
    if soup:
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

    print(f"Total registros: {len(records)} (1 historico + {len(records)-1} pronósticos)")
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
