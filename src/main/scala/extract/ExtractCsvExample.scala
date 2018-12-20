package extract


import org.apache.spark.sql.SparkSession

object ExtractCsvExample {

  val spark = SparkSession.builder().master("local[*]").appName("CSV_example").getOrCreate()
  spark.sparkContext.setLogLevel("Error")

  def main(args: Array[String]): Unit = {
    /**
      * In this example we extract a csv file. We pass several parameters using the option function.
      * The option function allows you to specify how your csv file is formatted.
      *
      * Here are several options (first paramater input is option, then second paramater is outcome
      *
      * header: true will take in first line for field names. False, will take in firs line as record and default field names to col1,col2....
      * inferSchema: false will make everything strings, true will infer the type for each column
      * delimiter: by default it's comma, but can also input "/t", " ", "/"
      * load: path to your csv file
     */
    println("Reading Csv file")
    val csvDf= spark.read.format("csv")
      // header can be set to true, if there is a head. or False if there is no header,
      // in which you will have to specifiy your own field names
      .option("header", "true")
      .option("inferschema", "false")
      .option("ignoreLeadingWhiteSpace", "true")
      .load("file/path/test.csv")

    // Print out infer schema
    csvDf.printSchema()
    // Print out top 20 rows
    csvDf.show()

  }

}
