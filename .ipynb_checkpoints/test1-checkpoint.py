from pyspark.sql import SparkSession

# Inicializar la sesión Spark
spark = SparkSession.builder \
    .appName("Spark Head Example") \
    .getOrCreate()

# Cargar el archivo de texto como un DataFrame
file_path = "/ruta/al/archivo.txt"
lines = spark.read.text(file_path)

# Mostrar las primeras filas usando la función head()
head_lines = lines.head(5)
print("Primeras 5 filas del archivo:")
for line in head_lines:
    print(line)

# Detener la sesión Spark
spark.stop()
