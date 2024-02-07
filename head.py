from pyspark.sql import SparkSession

# Inicializar la sesión Spark
spark = SparkSession.builder \
    .appName("Head Example") \
    .getOrCreate()

# Ruta del archivo u.data
file_path = "/Users/ndev/Desktop/SparkDev/u.data"

# Leer el archivo como un DataFrame
df = spark.read.option("delimiter", "\t").csv(file_path, inferSchema=True)

# Mostrar las primeras filas del DataFrame
df.show(5)

# Detener la sesión Spark
spark.stop()
