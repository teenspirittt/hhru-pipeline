import org.apache.spark.sql.SparkSession

object DataProcessing {
  def main(args: Array[String]): Unit = {
    val date = java.time.LocalDate.now().toString
    val date_vacancies = date
    val date_cleaned = date 

    val spark = SparkSession.builder
      .appName("DataProcessing")
      .master("spark://sparkmaster:7077")
      .config("spark.executor.memory", "1g")
      .config("spark.driver.host", "spark-master")
      .getOrCreate()

    val df = spark.read.json(s"hdfs://namenode:9000/hadoop-data/$date_vacancies.json")

    val cleanedDF = df.dropDuplicates()
      .filter(df("salary.from") > 0)
      .withColumn("salary.from", df("salary.from").cast("float"))
      .drop("response_letter_required")
      .na.drop("salary.from")
      .na.fill(0, Seq("salary.to"))

    cleanedDF.write.parquet(s"hdfs://namenode:9000/hadoop-data/processed/$date_cleaned.parquet")

    spark.stop()
  }
}
