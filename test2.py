from pyspark.sql import SparkSession

# Inicializar la sesión Spark
spark = SparkSession.builder \
    .appName("Spark Test") \
    .getOrCreate()

# Crear un DataFrame de ejemplo
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Mostrar las primeras filas del DataFrame
df.show()

# Detener la sesión de Spark
spark.stop()