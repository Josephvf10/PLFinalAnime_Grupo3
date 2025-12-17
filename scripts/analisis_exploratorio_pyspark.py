from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def make_spark():
    #Creamos la sesion de Spark
    return (
        SparkSession.builder
        .appName("AnimeEDA")
        #evita demasiadas particiones en local
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
"""
#Funcion para que spark pueda leer los csv sin romper comillas ni comas ni otros caracteres raros
    - encoding UTF-8: por caracteres raros/no ASCII.
    - quote/escape: porque hay títulos con comas y comillas en el CSV.
    - multiLine: por si hay campos que incluyen saltos de línea.
    - mode PERMISSIVE: si alguna fila viene rara, Spark intenta leerla sin romper todo.
"""
def read_csv(spark, path, header=True, infer=True):
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer).lower())
        .option("encoding", "UTF-8")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def main():
    #Se arranca spark
    spark = make_spark()
    #se ajusta el nivel de los logs
    spark.sparkContext.setLogLevel("WARN")


    # CARGA Y PREPROCESADO

    #Cargamos el catálogo de animes
    anime_df = read_csv(spark, "anime.csv")
    #Renombramos columnas para facilitar el analisis y los joins
    anime_df = (
        anime_df
        .withColumnRenamed("ID", "anime_id")
        .withColumnRenamed("Name", "name")
        .withColumnRenamed("English name", "english_name")
        .withColumnRenamed("Genres", "genres")
        .withColumnRenamed("Studios", "studios")
        .withColumnRenamed("Type", "type")
    )
    #Cargamos las valoraciones completas
    ratings_df = read_csv(spark, "rating_complete.csv")

    ep_df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv("valoraciones_EP.csv")
        .toDF("user_id", "anime_id", "rating")
    )

    # ANÁLISIS EXPLORATORIO

    #Sacamos la media y número de valoraciones por anime
    ratings_stats_df = (
        ratings_df
        .groupBy("anime_id")
        .agg(
            F.count("rating").alias("num_ratings"),
            F.avg("rating").alias("avg_rating")
        )
    )
    #Unimos las estadisticas de valoracion con la informacion del anime
    anime_ratings_df = ratings_stats_df.join(
        anime_df, on="anime_id", how="inner"
    )

    #MEJORES ANIMES

    #Mejores animes con al menos 1000 valoraciones
    print("\n=== TOP 10 MEJOR VALORADOS ===")
    (
        anime_ratings_df
        .filter(F.col("num_ratings") >= 1000)
        .orderBy(F.col("avg_rating").desc())
        .select("anime_id", "name", "english_name", "type", "avg_rating", "num_ratings")
        .show(10, truncate=60)
    )

    #PEORES ANIMES
    print("\n=== TOP 10 PEOR VALORADOS ===")
    (
        anime_ratings_df
        .filter(F.col("num_ratings") >= 1000)
        .orderBy(F.col("avg_rating").asc())
        .select("anime_id", "name", "english_name", "type", "avg_rating", "num_ratings")
        .show(10, truncate=60)
    )

    #Relacion genero – valoracion
    print("\n=== VALORACIÓN MEDIA POR GÉNERO ===")
    genres_df = (
        anime_ratings_df
        .withColumn("genre", F.explode(F.split(F.col("genres"), ", ")))
        .groupBy("genre")
        .agg(
            F.avg("avg_rating").alias("mean_rating"),
            F.count("*").alias("num_animes")
        )
        .orderBy(F.col("mean_rating").desc())
    )

    genres_df.show(15, truncate=60)

    #ESTUDOS MEJOR VALOADOS
    print("\n=== ESTUDIOS MEJOR VALORADOS ===")
    studios_df = (
        anime_ratings_df
        .withColumn("studio", F.explode(F.split(F.col("studios"), ", ")))
        .groupBy("studio")
        .agg(
            F.avg("avg_rating").alias("mean_rating"),
            F.count("*").alias("num_animes")
        )
        .filter(F.col("num_animes") >= 50)
        .orderBy(F.col("mean_rating").desc())
    )

    studios_df.show(15, truncate=60)
    #Cerramos la sesion de spark
    spark.stop()


if __name__ == "__main__":
    main()
