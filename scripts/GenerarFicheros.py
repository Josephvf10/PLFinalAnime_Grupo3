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

#GENERAR PDF
styles = getSampleStyleSheet()

def generar_pdf(nombre, titulo, datos):
    doc = SimpleDocTemplate(nombre)
    elementos = []

    elementos.append(Paragraph(titulo, styles["Heading2"]))
    tabla = Table(datos)
    tabla.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    elementos.append(tabla)
    doc.build(elementos)

generar_pdf(f"{SALIDA}series.pdf", "Series recomendadas", series)
generar_pdf(f"{SALIDA}peliculas.pdf", "Películas recomendadas", peliculas)

print("TXT y PDF generados correctamente en la carpeta 'salida/'")