from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def make_spark():
    #Creamos la sesion de Spark
    return (
        SparkSession.builder
        .appName("AnimeRecs-Step1-Load")
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
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

def main():
    #Se arranca spark
    spark = make_spark()
    #se ajusta el nivel de los logs
    spark.sparkContext.setLogLevel("WARN")
    #cargamos catalogo anime csv
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
    #cargamos las valoraciones completas
    ratings_df = read_csv(spark, "rating_complete.csv")
    ep_df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv("valoraciones_EP.csv")
        .toDF("user_id", "anime_id", "rating")
    )

    #Hicimos comprobaciones para ver si todo se lee bien
    print("\n=== FILAS (conteo) ===")
    print("anime:", anime_df.count())
    print("ratings:", ratings_df.count())
    print("ep:", ep_df.count())

    print("\n=== ESQUEMAS ===")
    anime_df.printSchema()
    ratings_df.printSchema()
    ep_df.printSchema()

    print("\n=== MUESTRAS ===")
    anime_df.select("anime_id", "name", "english_name", "type").show(5, truncate=60)
    ratings_df.show(5, truncate=60)
    ep_df.show(5, truncate=60)

    #Cerramos la sesion de spark
    spark.stop()

if __name__ == "__main__":
    main()
