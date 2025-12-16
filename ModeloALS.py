
#Crear la sesion en Spark
spark = SparkSession.builder.appName("PLFinal_ALS").getOrCreate()

#para limpiar la salida en Spark, establecer el nivel de logs a ERROR
spark.sparkContext.setLogLevel("ERROR")

#lectuira de csv
ratings_com = spark.read.option("header", "true").csv("rating_complete.csv")
valoracionesEP = spark.read.option("header", "true").csv("valoraciones_EP.csv")
anime  = spark.read.option("header", "true").csv("anime.csv")
