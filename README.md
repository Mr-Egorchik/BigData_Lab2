# BigData_Lab2

Читоркин Егор, 6133-010402D

Отчеты в формате parquet хранятся в папке reports

### Ход работы

Для начала считаем данные из списка языков программирования
```scala
val data_lang = spark.read.option("header", value = true).csv("programming-languages.csv")
```

Далее извлечем оттуда столбец названий языков программирования и переведем их в нижний регистр
```scala
val languages = data_lang.withColumn("name", trim(lower(data_lang("name")))).select("name").rdd.map(_.getString(0)).collect().toList
```

Считаем теперь данные о постах из файла формата xml

```scala
val posts = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load("posts_sample.xml")
```

Далее с помощью SQL выгрузим из полученного датафрейма нужные нам столбцы (id, tag, year), причем сразу учтем только те годы, которые нам интересны. После чего применим операцию редукции +, проведем фильтрацию по языкам, которые есть в списке языков. После этого произведем маппинг до приемлемого вида (год-язык-количество).
```scala
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
```

Далее с помощью SQL для каждого года сделаем выборки по 10 самых популярных языков.
```scala
    val result = (2010 until 2021).map(
      i => spark.sql(s"SELECT * FROM postsYearLangCount WHERE year = $i ORDER BY year ASC, count DESC LIMIT 10")
    ).reduce((df1, df2) => df1.union(df2))
```

С помощью функции write.parquet запишем эти отчеты в файлы.

В jupyter-notebook с помощью библиотеки pandas можно посмотреть содержимое этих отчетов (см. файл read.ipynb).


