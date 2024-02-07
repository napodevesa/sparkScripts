from pyspark import SparkConf, SparkContext

# Verificar si ya existe un SparkContext
if 'sc' not in globals():
    # Configuración de Spark
    conf = SparkConf().setMaster("local").setAppName("RatingHist")
    sc = SparkContext(conf=conf)

# Lectura del archivo de datos
lines = sc.textFile("/Users/ndev/Desktop/SparkDev/u.data")

# Extracción de las calificaciones y conteo de ocurrencias
ratings = lines.map(lambda x: x.split()[2])  # Se extrae la calificación de cada línea
result = ratings.countByValue()  # Se cuenta la ocurrencia de cada calificación

# Ordenamiento de los resultados
sortedResults = sorted(result.items())  # Se ordenan los resultados por calificación

# Impresión de los resultados
for key, value in sortedResults:
    print("%s %i" % (key, value))  # Se imprime cada calificación junto con su ocurrencia

# No es necesario detener el SparkContext aquí, ya que este código se ejecutará en un entorno interactivo como Jupyter Notebook o IPython
