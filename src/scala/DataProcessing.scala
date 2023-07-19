import org.apache.spark.sql.SparkSession

object DataProcessing {
  def main(args: Array[String]): Unit = {
   
    val date = java.time.LocalDate.now().toString

    val date_vacancies = s"$date" + "_vacancies"
    val date_cleaned = s"$date" + "_vacancies_cleaned"

    val spark = SparkSession.builder
      .appName("DataProcessing") 
      .master("spark://sparkmaster:7077") 
      .getOrCreate()


    val df = spark.read.json(s"hdfs://namenode:9000/hadoop-data/$date_vacancies.json")

   
 val cleanedDF = df.dropDuplicates()
      .filter(df("salary.from") > 0)
      .withColumn("salary.from", df("salary.from").cast("float"))
      .drop("response_letter_required")
      .na.drop(Seq("salary.from")) 
      .na.fill(0, Seq("salary.to"))

   
    cleanedDF.write.mode("overwrite").parquet(s"hdfs://namenode:9000/hadoop-data/$date_cleaned.parquet")

    
    spark.stop()
  }
}
