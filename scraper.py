import os
import sys
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
URL_30RATES = "https://30rates.com/usd-cop"

# User-Agent que confirmadamente funciona contra 30rates
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0 Safari/537.36"
    )
}


def parse_fecha(texto_fecha: str, fecha_captura: date):
    """Convierte 'DD/MM' a fecha completa, manejando cambio de año."""
    try:
        dia, mes = texto_fecha.split("/")
        dia, mes = int(dia), int(mes)
        anio = fecha_captura.year
        # Si el mes es menor al actual, asumimos próximo año
        if mes < fecha_captura.month:
            anio += 1
        fecha = date(anio, mes, dia)
        return fecha
    except (ValueError, IndexError):
        return None


def limpiar_numero(texto):
    """Convierte texto numérico a int, eliminando comas/símbolos."""
    try:
        limpio = texto.replace(",", "").replace("$", "").strip()
        return int(float(limpio))
    except (ValueError, AttributeError):
        return None


def scrape_30rates():
    print(f"Fetching {URL_30RATES}...")
    response = requests.get(URL_30RATES, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # La tabla diaria de 30rates tiene class="tbh"
    tabla = soup.find("table", class_="tbh")
    if tabla is None:
        raise RuntimeError("No se encontró la tabla con class 'tbh'")

    fecha_captura = date.today()
    registros = []

    for tr in tabla.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != 5:
            continue

        celdas = [td.get_text(strip=True) for td in tds]

        # Saltar fila de encabezado (Date|Weekday|Min|Max|Rate)
        if celdas[0].lower() == "date":
            continue

        fecha_pron = parse_fecha(celdas[0], fecha_captura)
        if fecha_pron is None:
            continue

        min_cop = limpiar_numero(celdas[2])
        max_cop = limpiar_numero(celdas[3])
        rate_cop = limpiar_numero(celdas[4])
        if None in (min_cop, max_cop, rate_cop):
            continue

        dia_semana = celdas[1].strip()
        tipo = "historico" if fecha_pron < fecha_captura else "pronostico"

        # Fila principal (lunes a viernes según la tabla)
        registros.append({
            "fecha_captura": fecha_captura.isoformat(),
            "fecha_pronostico": fecha_pron.isoformat(),
            "dia_semana": dia_semana,
            "min_cop": min_cop,
            "max_cop": max_cop,
            "rate_cop": rate_cop,
            "tipo": tipo,
        })

        # Si es viernes, generar también sábado y domingo (heredan la misma tasa)
        if dia_semana.lower() == "friday":
            for offset, nombre in [(1, "Saturday"), (2, "Sunday")]:
                fecha_finde = fecha_pron + timedelta(days=offset)
                tipo_finde = "historico" if fecha_finde < fecha_captura else "pronostico"
                registros.append({
                    "fecha_captura": fecha_captura.isoformat(),
                    "fecha_pronostico": fecha_finde.isoformat(),
                    "dia_semana": nombre,
                    "min_cop": min_cop,
                    "max_cop": max_cop,
                    "rate_cop": rate_cop,
                    "tipo": tipo_finde,
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
    print(f"Primer registro: {datos[0]}")
    print(f"Último registro: {datos[-1]}")

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
