from pyspark import Row
from pyspark.ml.recommendation import ALS
import time
import requests

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from rich.console import Console

import os
import shutil

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

print()
print("MODELO: ENTRENAMIENTO")
#Parte 2: MODELO ALS
print("Entrenando el modelo...")
als=ALS(
    userCol="user_id",
    itemCol="anime_id",
    ratingCol="rating",
    rank=8,
    maxIter=3,
    regParam=0.15,
    coldStartStrategy="drop",
)
modelo = als.fit(ratings_total)

print("Entrenamiento completado.")

#como ALS trabaja con dataframes, hay que crear uno para el usuario 666666:
usuario = spark.createDataFrame([Row(user_id=666666 )])

#obtener recomendaciones para el usuario
recomendaciones_ep = modelo.recommendForUserSubset(usuario, 50)


recomendaciones_ep = recomendaciones_ep.select(
    f.col("user_id"),
    f.explode("recommendations").alias("rec")
)

#seleccionar solo las columnas necesarias para las recomendaciones finales:'anime_id' (ID del anime) y 'rating' (valoración predicha del modelo ALS).
recomendaciones_ep = recomendaciones_ep.select(
    "user_id",
    f.col("rec.anime_id").alias("anime_id"),
    f.col("rec.rating").alias("predicted_rating")
)

#unir con la info de anime
anime = anime.select(
    f.col("ID").cast("int").alias("anime_id"),
    f.col("Name").alias("name"),
    f.col("English name").alias("english_name"),
    f.col("Type").alias("type")
)
#unir las recomendaciones con los detalles de los animes (nombre, tipo, etc.) usando 'anime_id' como clave de unión (INNER JOIN).
recs_ep = recomendaciones_ep.join(anime, on="anime_id", how="inner")

#print(" RECOMENDACIONES PARA EL USUARIO 666666 (vista previa)")
#recs_ep.show(truncate=False)

#excluir aquellos animes ya valorados por el usuario de la recomendación:
valorados_ep = (
    ratings_total
    .filter(f.col("user_id") == 666666)
    .select("anime_id")
    .distinct()
)

#animes recomendados que ya estaban valorados
recs_con_valorados = recs_ep.join(
    valorados_ep,
    on="anime_id",
    how="inner"
)

#exclusión definitiva de animes ya valorados
recs_ep = recs_ep.join(
    valorados_ep,
    on="anime_id",
    how="left_anti"
)

#SEPARAR SERIES Y PELÍCULAS
#SERIES: su tipo es TV
recs_tv = (
    recs_ep
    .filter(f.col("type") == "TV")
    .orderBy(f.col("predicted_rating").desc())
    .limit(5)
)
print()
print("RECOMENDACIONES: ")
print(" RECOMENDACIONES TV")
recs_tv.show(truncate=False)

# PELÍCULAS: su tipo es Movie
recs_movie = (
    recs_ep
    .filter(f.col("type") == "Movie")
    .orderBy(f.col("predicted_rating").desc())
    .limit(5)
)

print("RECOMENDACIONES PELÍCULAS")
recs_movie.show(truncate=False)


#GUARDAR RESULTADOS EN CSV
#columnas que tendrá el csv
cols_salida = [
    "anime_id",
    "name",
    "english_name",
    f.round(f.col("predicted_rating"), 2).alias("rating")
]

print()
print("GUARDANDO CSV...")

#eliminar carpetas de salida si existen (para evitar errores en ejecuciones repetidas)
ruta_series = "../salida/recomendaciones_series"
ruta_peliculas = "../salida/recomendaciones_peliculas"

if os.path.exists(ruta_series):
    shutil.rmtree(ruta_series)

if os.path.exists(ruta_peliculas):
    shutil.rmtree(ruta_peliculas)

#series
recs_tv_csv = (
    recs_tv
    .select(*cols_salida)
)


#para guardar todo en un archivo
recs_tv_csv.coalesce(1).write.option("header", "true").csv(ruta_series)

#peliculas
recs_movie_csv = (
    recs_movie
    .select(*cols_salida)
)

#para guardar todo en un archivo
recs_movie_csv.coalesce(1).write.option("header", "true").csv(ruta_peliculas)

print("CSV generados en carpeta 'salida/'")





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

