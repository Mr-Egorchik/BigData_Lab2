# BigData_Lab2

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
