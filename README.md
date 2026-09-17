# Taller 1 – ETL con PySpark (Online Retail Dataset)

Proceso ETL con PySpark sobre el dataset **Online Retail** (UCI Machine
Learning Repository), con selección, filtros, agregaciones, agrupaciones,
columnas derivadas, funciones de ventana y joins.

**Autor:** Carlos Andres Taquez Maya — ID: 844173
**Curso:** Sexto semestre – Ingeniería de software

## 1. Requisitos previos

- Python 3.9+
- Java 8 u 11 (requerido por Spark)
- PySpark

Instalación rápida:
```bash
pip install pyspark pandas
```

## 2. Descargar el dataset

Opción automática (recomendada), usando `descargar_dataset.py`:
```bash
pip install ucimlrepo pandas openpyxl requests
python descargar_dataset.py
```

Opción manual:
1. Ir a: https://archive.ics.uci.edu/dataset/352/online+retail
2. Descargar `Online Retail.xlsx`
3. Exportarlo como CSV (Excel/LibreOffice/Google Sheets, o con pandas:
   `pd.read_excel(...).to_csv(...)`)
4. Guardarlo como:
