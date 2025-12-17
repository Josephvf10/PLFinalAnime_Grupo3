import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from rich.console import Console

#Crear la sesion en Spark
spark = SparkSession.builder.appName("PLFinal_ALS").getOrCreate()

#para limpiar la salida en Spark, establecer el nivel de logs a ERROR
spark.sparkContext.setLogLevel("ERROR")

#lectuira de csv
ratings_com = spark.read.option("header", "true").csv("rating_complete.csv")
valoracionesEP = spark.read.option("header", "false").csv("valoraciones_EP.csv")
anime  = spark.read.option("header", "true").csv("anime.csv")

#PRIMERA PARTE: unir ratings del usuario 666666 al archivo de ratings general
#preprocesamiento: convertir a entero las columnas que se van a utilizar
ratings_com = ratings_com.select(
    f.col("user_id").cast("int"),
    f.col("anime_id").cast("int"),
    f.col("rating").cast("int"),
)

#comprobación de que se ha cambiado a int
print("Esquema de ratings_complete:")
ratings_com.printSchema()

#como el csv de valoraciones no tiene columnas, las creamos:
valoracionesEP = valoracionesEP.toDF("user_id", "anime_id", "rating")

valoracionesEP = valoracionesEP.select(
    f.col("user_id").cast("int"),
    f.col("anime_id").cast("int"),
    f.col("rating").cast("int"),
)
#comprobación:
print("Esquema de valoraciones_EP:")
valoracionesEP.printSchema()

print("Filas totales en valoracionesEP:")
print(valoracionesEP.count())

print("Comprobación de si ya hay valoraciones del usuario 666666 en ratings_complete:")

num_ep = ratings_com.filter(f.col("user_id") == 666666).count()
print(f"Valoraciones del usuario EP en el dataset inicial: {num_ep}")

#como no hay, añadimos valoracionesEP a ratings_com:
print("Añadiendo valoraciones del usuario 666666...")
ratings_total= ratings_com.unionByName(valoracionesEP)

#Verficicación de que las valoraciones de EP ya están en el dataset final:
num_ep = ratings_total.filter(f.col("user_id") == 666666).count()
print(f"Valoraciones del usuario EP en el dataset final: {num_ep}")

#Configuración de la API
DIR_SALIDA = "../salida"
API_URL = "https://api.jikan.moe/v4/anime/"
API_DELAY = 1.0
console = Console()

def obtener_datos (anime_id):
    url = f"{API_URL}{anime_id}"
    time.sleep(API_DELAY)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('data', {})
        image_url = data.get("images", {}).get("jpg", {}).get("large_image_url")
        trailer_url = data.get("trailer", {}).get("url") or data.get("trailer", {}).get("embed_url")

        return {
            "title_english": data.get("title_english"),
            "synopsis": data.get("synopsis", "Información no disponible"),
            "genres": [g['name'] for g in data.get("genres", [])],
            "studios": [s['name'] for s in data.get("studios", [])],
            "episodes": data.get("episodes", "Información no disponible"),
            "status": data.get("status", "Información no disponible"),
            "score": data.get("score", "Información no disponible"),
            "image_url": image_url or "Información no disponible",
            "trailer_url": trailer_url or "Información no disponible"
        }
    except Exception as e:
        return {"error": str(e)}