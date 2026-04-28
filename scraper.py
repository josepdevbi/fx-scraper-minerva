import os
import sys
import cloudscraper
from bs4 import BeautifulSoup
from datetime import date
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
URL_30RATES = "https://30rates.com/usd-cop"


def parse_fecha(texto_fecha: str, fecha_captura: date):
    """Convierte 'DD/MM' a fecha completa, manejando cambio de año."""
    try:
        dia, mes = texto_fecha.split("/")
        dia, mes = int(dia), int(mes)
        anio = fecha_captura.year
        fecha = date(anio, mes, dia)
        if (fecha_captura - fecha).days > 180:
            fecha = date(anio + 1, mes, dia)
        return fecha
    except (ValueError, IndexError):
        return None


def es_fila_encabezado(celdas):
    """Detecta la fila Date|Weekday|Min|Max|Rate (puede venir en <th> o <td>)."""
    if len(celdas) < 5:
        return False
    esperados = {"date", "weekday", "min", "max", "rate"}
    valores = {c.strip().lower() for c in celdas[:5]}
    return esperados.issubset(valores)


def scrape_30rates():
    # cloudscraper se hace pasar por un navegador real
    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "desktop": True,
        }
    )
    response = scraper.get(URL_30RATES, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    fecha_captura = date.today()
    registros = []

    for tabla in soup.find_all("table"):
        filas = tabla.find_all("tr")
        if len(filas) < 2:
            continue

        primera_fila_celdas = [
            c.get_text(strip=True) for c in filas[0].find_all(["td", "th"])
        ]
        if not es_fila_encabezado(primera_fila_celdas):
            continue

        for fila in filas[1:]:
            celdas = [c.get_text(strip=True) for c in fila.find_all(["td", "th"])]
            if len(celdas) < 5:
                continue

            if "/" not in celdas[0]:
                continue

            fecha_pron = parse_fecha(celdas[0], fecha_captura)
            if fecha_pron is None:
                continue

            try:
                min_cop = int(celdas[2])
                max_cop = int(celdas[3])
                rate_cop = int(celdas[4])
            except ValueError:
                continue

            tipo = "historico" if fecha_pron < fecha_captura else "pronostico"

            registros.append({
                "fecha_captura": fecha_captura.isoformat(),
                "fecha_pronostico": fecha_pron.isoformat(),
                "dia_semana": celdas[1],
                "min_cop": min_cop,
                "max_cop": max_cop,
                "rate_cop": rate_cop,
                "tipo": tipo,
            })

    # Deduplicar
    vistos = set()
    unicos = []
    for r in registros:
        clave = (r["fecha_captura"], r["fecha_pronostico"])
        if clave not in vistos:
            vistos.add(clave)
            unicos.append(r)

    return unicos


def main():
    print(f"Iniciando scrape: {date.today().isoformat()}")

    try:
        datos = scrape_30rates()
    except Exception as e:
        print(f"Error en scraping: {e}", file=sys.stderr)
        sys.exit(1)

    if not datos:
        print("No se encontraron datos. Posible cambio de estructura HTML.", file=sys.stderr)
        sys.exit(1)

    print(f"Registros parseados: {len(datos)}")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    try:
        result = supabase.table("usd_cop_30rates").upsert(
            datos,
            on_conflict="fecha_captura,fecha_pronostico"
        ).execute()
        print(f"Upsert exitoso: {len(result.data)} filas afectadas")
    except Exception as e:
        print(f"Error en upsert: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
