import csv
import os
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

SALIDA = "../salida/"

#buscar el csv de spark
def buscar_csv(carpeta):
    for f in os.listdir(carpeta):
        if f.startswith("part-") and f.endswith(".csv"):
            return os.path.join(carpeta, f)
    raise FileNotFoundError(f"No se encontró CSV en {carpeta}")

SERIES_CSV = buscar_csv("../salida/recomendaciones_series")
PELICULAS_CSV = buscar_csv("../salida/recomendaciones_peliculas")

os.makedirs(SALIDA, exist_ok=True)

def leer_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))

series = leer_csv(SERIES_CSV)
peliculas = leer_csv(PELICULAS_CSV)

#GENERAR TXT
with open(f"{SALIDA}series.txt", "w", encoding="utf-8") as f:
    for fila in series:
        f.write(" | ".join(fila) + "\n")

with open(f"{SALIDA}peliculas.txt", "w", encoding="utf-8") as f:
    for fila in peliculas:
        f.write(" | ".join(fila) + "\n")

