import pandas as pd

spark = SparkSession.builder.appName("PeliculasGithub").getOrCreate()

url = 'https://raw.githubusercontent.com/14DAVEN18/data-imdb/main/imdb-movies-dataset.csv'

pd_df = pd.read_csv(url)
spark_df = spark.createDataFrame(pd_df)

peliculaExitosa = spark_df.dropna (subset="Rating") \
   .filter(spark_df.Rating > 7) \
   .dropna (subset="Year") \
   .dropna (subset="Duration (min)") \
   .filter(spark_df. Year > 2014) \
   .groupBy(spark_df.Genre) \
   .mean("Duration (min)") \
   .orderBy("avg (Duration (min))", ascending=False)

peliculaPandas = peliculaExitosa.toPandas()

peliculaPandas.to_csv('C:/Users/miton/Downloads/peliculas-filtradas.csv')

spark.stop()