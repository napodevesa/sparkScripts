from pyspark.sql import SparkSession

# Inicializar la sesión Spark
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .getOrCreate()

# Imprimir "Hello World"
print("Hello World")

# Verificar que Spark funciona
df = spark.range(0, 5)
print("Spark está funcionando correctamente. Este es un DataFrame de ejemplo:")
df.show()

# Detener la sesión Spark
spark.stop()
