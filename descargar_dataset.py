"""
Descarga del dataset "Online Retail" (UCI Machine Learning Repository)
========================================================================
Este script descarga el dataset y lo guarda como CSV en:
    data/OnlineRetail.csv

Requiere conexión a internet (se ejecuta en TU máquina local, no en Spark).

Instalación de dependencias:
    pip install ucimlrepo pandas openpyxl requests

Uso:
    python descargar_dataset.py
"""

import os
import sys

DATA_DIR = "data"
OUTPUT_CSV = os.path.join(DATA_DIR, "OnlineRetail.csv")

os.makedirs(DATA_DIR, exist_ok=True)


def metodo_1_ucimlrepo():
    """
    Método recomendado: usa el paquete oficial 'ucimlrepo', que se conecta
    directamente a la API de UCI y no depende de una URL de archivo que
    pueda cambiar.
    """
    from ucimlrepo import fetch_ucirepo
    import pandas as pd

    print("Descargando dataset vía ucimlrepo (id=352: Online Retail)...")
    online_retail = fetch_ucirepo(id=352)

    # 'features' trae las columnas del dataset (no tiene 'targets' porque
    # no es un dataset de clasificación/regresión supervisada)
    df = online_retail.data.features.copy()

    # Si además vienen ids (InvoiceNo, StockCode) en 'data.ids', los unimos
    if online_retail.data.ids is not None:
        df = pd.concat([online_retail.data.ids, df], axis=1)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Dataset guardado en: {OUTPUT_CSV}  ({len(df)} filas)")
    return True


def metodo_2_descarga_directa():
    """
    Método alternativo: descarga directamente el archivo .xlsx original
    desde el sitio de UCI y lo convierte a CSV con pandas.
    """
    import requests
    import pandas as pd

    url = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
    xlsx_path = os.path.join(DATA_DIR, "Online_Retail.xlsx")
    zip_path = os.path.join(DATA_DIR, "online_retail.zip")

    print(f"Descargando ZIP desde: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with open(zip_path, "wb") as f:
        f.write(r.content)

    import zipfile
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DATA_DIR)
        print("Archivos extraídos:", z.namelist())

    # Busca el .xlsx extraído
    xlsx_files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".xlsx")]
    if not xlsx_files:
        raise FileNotFoundError("No se encontró el .xlsx dentro del ZIP descargado.")

    xlsx_real_path = os.path.join(DATA_DIR, xlsx_files[0])
    print(f"Convirtiendo {xlsx_real_path} a CSV...")

    df = pd.read_excel(xlsx_real_path)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Dataset guardado en: {OUTPUT_CSV}  ({len(df)} filas)")
    return True


def main():
    try:
        metodo_1_ucimlrepo()
        return
    except Exception as e:
        print(f"[Aviso] Falló el método con ucimlrepo: {e}")
        print("Intentando método alternativo de descarga directa...\n")

    try:
        metodo_2_descarga_directa()
        return
    except Exception as e:
        print(f"[Error] También falló la descarga directa: {e}")
        print(
            "\nDescarga manual como último recurso:\n"
            "1. Entra a https://archive.ics.uci.edu/dataset/352/online+retail\n"
            "2. Da clic en 'Download' para obtener 'Online Retail.xlsx'\n"
            "3. Ábrelo en Excel/LibreOffice y guárdalo como CSV\n"
            f"4. Colócalo en: {OUTPUT_CSV}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()