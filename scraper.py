import os
import sys
import requests
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
        # Si la fecha resulta muy anterior, asumimos que es del próximo año
        # (caso: capturamos en diciembre y la tabla muestra enero)
        if (fecha_captura - fecha).days > 180:
            fecha = date(anio + 1, mes, dia)
        return fecha
    except (ValueError, IndexError):
        return None


def scrape_30rates():
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
    }
    response = requests.get(URL_30RATES, headers=headers, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    fecha_captura = date.today()
    registros = []
    columnas_esperadas = {"Date", "Weekday", "Min", "Max", "Rate"}

    for tabla in soup.find_all("table"):
        encabezados = {th.get_text(strip=True) for th in tabla.find_all("th")}
        if not columnas_esperadas.issubset(encabezados):
            continue

        for fila in tabla.find_all("tr")[1:]:
            celdas = [td.get_text(strip=True) for td in fila.find_all("td")]
            if len(celdas) < 5:
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

    # Deduplicar por (fecha_captura, fecha_pronostico)
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
