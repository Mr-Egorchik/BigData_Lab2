import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{lower, trim}
import org.apache.spark.{SparkConf, SparkContext}

case class LangYear(lang: String, year: Int)

object Main {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("Hello world!")

    val appName = "BigData2"
    val master = "local[2]"

    val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()

    val data_lang = spark.read.option("header", value = true).csv("programming-languages.csv")
    val languages = data_lang.withColumn("name", trim(lower(data_lang("name")))).select("name").rdd.map(_.getString(0)).collect().toList
    println(languages.take(10))

    val posts = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load("posts_sample.xml")

    import spark.implicits._

    posts.createOrReplaceTempView("posts")

    println("posts created")

    val postsYearLangCount = spark.sql("SELECT _Id as id, _Tags as tags, EXTRACT(year from _CreationDate) as year FROM posts WHERE _CreationDate BETWEEN cast('2010-01-01' as timestamp) and cast('2021-01-01' as timestamp) and _Tags is not null")
      .rdd
      .flatMap(
        row => row.getString(1).strip().drop(1).dropRight(1)
          .split("><")
          .map(s => (LangYear(s, row.getInt(2)), 1L)))
      .reduceByKey(_ + _)
      .filter(l => languages.contains(l._1.lang))
      .map(s => (s._1.year, s._1.lang, s._2))
      .toDF("year", "language", "count")

    println("tags counted")

    postsYearLangCount.createOrReplaceTempView("postsYearLangCount")

    val result = (2010 until 2021).map(
      i => spark.sql(s"SELECT * FROM postsYearLangCount WHERE year = $i ORDER BY year ASC, count DESC LIMIT 10")
    ).reduce((df1, df2) => df1.union(df2))

    result.show(100)
    result.write.parquet("report")

    spark.stop()

  }
}