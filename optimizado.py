import requests
import pandas as pd
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import os

API_KEY = 'a6f59ba2-f7fc-4bd0-9b88-70eae14dd55e'
BASE_URL = 'https://apiv1.foxtrotsystems.com'
DC_IDS = ["BO016", "BO77", "BO079", "BO009", "BO019", "BO020", "BO095", "BO100", "BO011", "BO088", "BO007"]
FECHA = date.today().isoformat()

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Crear sesión persistente optimizada
session = requests.Session()
retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Archivo CSV final
OUTPUT_FILE = f"entregas_{FECHA}.csv"

def safe_get(url):
    try:
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"⚠️ Error al conectar con {url}: {e}")
        return None

def fetch_deliveries(url, dc_id, ruta_name, customer_id):
    d = safe_get(url)
    if not d:
        return []
    deliveries = d.json().get('data', {}).get('deliveries', [])
    if not deliveries:
        return [{
            "dc_id": dc_id,
            "ruta_name": ruta_name,
            "customer_id": customer_id,
            "attempt_status": "BLANK",
            "product_id": "BLANK"
        }]

    resultados = []
    for entrega in deliveries:
        delivery_id = entrega.get("id", "BLANK")
        product_id = delivery_id.split("_")[1] if "_" in delivery_id else delivery_id
        attempts = entrega.get("attempts", [])
        if not attempts:
            resultados.append({
                "dc_id": dc_id,
                "ruta_name": ruta_name,
                "customer_id": customer_id,
                "attempt_status": "BLANK",
                "product_id": product_id
            })
        else:
            for intento in attempts:
                resultados.append({
                    "dc_id": dc_id,
                    "ruta_name": ruta_name,
                    "customer_id": customer_id,
                    "attempt_status": intento.get("attempt_status", "BLANK"),
                    "product_id": product_id
                })
    return resultados

def procesar_waypoints(ruta, dc_id):
    ruta_id = ruta.get('id', 'BLANK')
    ruta_name = ruta.get('name', 'BLANK')

    url_waypoints = f'{BASE_URL}/dcs/{dc_id}/routes/{ruta_id}/waypoints'
    r = safe_get(url_waypoints)
    if not r:
        return []

    waypoints = r.json().get('data', {}).get('waypoints', [])
    if not waypoints:
        return [{
            "dc_id": dc_id,
            "ruta_name": ruta_name,
            "customer_id": "BLANK",
            "attempt_status": "BLANK",
            "product_id": "BLANK"
        }]

    resultados = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = []
        for wp in waypoints:
            waypoint_id = wp.get("waypoint_id", "BLANK")
            customer_id = wp.get("customer_id", "BLANK")
            url_deliveries = f'{BASE_URL}/dcs/{dc_id}/routes/{ruta_id}/waypoints/{waypoint_id}/deliveries'
            futures.append(executor.submit(fetch_deliveries, url_deliveries, dc_id, ruta_name, customer_id))
        for future in as_completed(futures):
            resultados.extend(future.result())
    return resultados

def procesar_dc(dc_id):
    url_rutas = f'{BASE_URL}/dcs/{dc_id}/routes/find_by_date/{FECHA}'
    response = safe_get(url_rutas)
    if not response:
        return []

    rutas = response.json().get('data', {}).get('routes', [])
    entregas_planas = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(procesar_waypoints, ruta, dc_id) for ruta in rutas]
        for future in as_completed(futures):
            entregas_planas.extend(future.result())
    return entregas_planas

# Ejecutar todos los DCs en paralelo
resultados_totales = []
with ThreadPoolExecutor(max_workers=len(DC_IDS)) as executor:
    futures = [executor.submit(procesar_dc, dc_id) for dc_id in DC_IDS]
    for future in as_completed(futures):
        resultados_totales.extend(future.result())

# Crear DataFrame y guardar CSV final
df = pd.DataFrame(resultados_totales)
for col in ["dc_id", "ruta_name", "customer_id", "attempt_status", "product_id"]:
    if col not in df.columns:
        df[col] = "BLANK"
    df[col] = df[col].fillna("BLANK")

df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"\n✅ Ejecución completada. Datos guardados en: {OUTPUT_FILE}")
