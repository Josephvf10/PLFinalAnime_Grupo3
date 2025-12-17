from pyspark import Row
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

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

#Parte 2: MODELO ALS
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

#como ALS trabaja con dataframes, hay que crear uno para el usuario 666666:
usuario = spark.createDataFrame([Row(user_id=666666 )])

#obtener recomendaciones para el usuario
recomendaciones_ep = modelo.recommendForUserSubset(usuario, 20)


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

print("\n=== RECOMENDACIONES PARA EL USUARIO 666666 === (vista previa)")
recs_ep.show(truncate=False)
