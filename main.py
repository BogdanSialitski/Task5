from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, unix_timestamp, datediff



spark = SparkSession.builder \
    .appName("Postgres to PySpark") \
.config("spark.driver.extraClassPath", "C:/hadoop/lib/postgresql-42.7.3.jar")   \
    .getOrCreate()


url = "jdbc:postgresql://localhost/a"
properties = {
    "user": "postgres",
    "password": "13BD13",
    "driver": "org.postgresql.Driver"
}

actors_df = spark.read.jdbc(url=url, table="actor", properties=properties)
film_actor_df = spark.read.jdbc(url=url, table="film_actor", properties=properties)
film_df = spark.read.jdbc(url=url, table="film", properties=properties)
inventory_df = spark.read.jdbc(url=url, table="inventory", properties=properties)
rental_df = spark.read.jdbc(url=url, table="rental", properties=properties)
film_category_df = spark.read.jdbc(url=url, table="film_category", properties=properties)
category_df = spark.read.jdbc(url=url, table="category", properties=properties)
payment_df = spark.read.jdbc(url=url, table="payment", properties=properties)
city_df = spark.read.jdbc(url=url, table="city", properties=properties)
address_df = spark.read.jdbc(url=url, table="address", properties=properties)
customer_df = spark.read.jdbc(url=url, table="customer", properties=properties)

#task1 -
print("Вывести количество фильмов в каждой категории, отсортировать по убыванию")
result_df = film_category_df \
    .join(category_df, film_category_df.category_id == category_df.category_id) \
    .groupBy(category_df.name) \
    .count() \
    .withColumnRenamed("count", "count_films") \
    .orderBy(col("count_films").desc())


result_df.show()


#task2
print("Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.")
actor_rentals = actors_df \
    .join(film_actor_df, actors_df.actor_id == film_actor_df.actor_id) \
    .join(film_df, film_actor_df.film_id == film_df.film_id) \
    .join(inventory_df, film_df.film_id == inventory_df.film_id) \
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id) \
    .groupBy(actors_df.first_name, actors_df.last_name) \
    .agg(count(rental_df.rental_id).alias("rental_count")) \
    .orderBy(col("rental_count").desc()) \
    .limit(10)

actor_rentals.show()

#task3
print("Вывести категорию фильмов, на которую потратили больше всего денег")

category_spending = payment_df \
    .join(rental_df, payment_df.rental_id == rental_df.rental_id) \
    .join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id) \
    .join(film_category_df, inventory_df.film_id == film_category_df.film_id) \
    .join(category_df, film_category_df.category_id == category_df.category_id) \
    .groupBy(category_df.name) \
    .agg(sum(col("amount")).alias("total_amount")) \
    .orderBy(col("total_amount").desc()) \
    .limit(1)

category_spending.show()

#task4
print("Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.")
films_not_in_inventory = film_df \
    .join(inventory_df, film_df.film_id == inventory_df.film_id, how='left') \
    .filter(inventory_df.inventory_id.isNull()) \
    .select(film_df.title)

films_not_in_inventory.show()

#task5
print("Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.")
actors_in_children_category = actors_df \
    .join(film_actor_df, actors_df.actor_id == film_actor_df.actor_id) \
    .join(film_df, film_actor_df.film_id == film_df.film_id) \
    .join(film_category_df, film_df.film_id == film_category_df.film_id) \
    .join(category_df, film_category_df.category_id == category_df.category_id) \
    .filter(category_df.name == "Children") \
    .groupBy(actors_df.first_name, actors_df.last_name) \
    .agg(count(film_df.film_id).alias("film_count")) \
    .orderBy(col("film_count").desc()).limit(3)

top_3_film_count = actors_in_children_category.limit(3).select(col("film_count")).collect()[-1][0]

top_actors = actors_in_children_category.filter(col("film_count") >= top_3_film_count)

top_actors.show()
#task6
print('--Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.')
city_address_customer_df = city_df \
    .join(address_df, city_df.city_id == address_df.city_id, how='left') \
    .join(customer_df, address_df.address_id == customer_df.address_id, how='left')
result_df = city_address_customer_df \
    .groupBy("city") \
    .agg(
        count(when(col("active") == 1, 1)).alias("active"),
        count(when(col("active") == 0, 1)).alias("inactive")
    ) \
    .orderBy(col("inactive").desc())

result_df.show()

#task7
print("--Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.")
joined_df = category_df \
    .join(film_category_df, category_df.category_id == film_category_df.category_id) \
    .join(inventory_df, film_category_df.film_id == inventory_df.film_id) \
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id) \
    .join(customer_df, rental_df.customer_id == customer_df.customer_id) \
    .join(address_df, customer_df.address_id == address_df.address_id) \
    .join(city_df, address_df.city_id == city_df.city_id)

filtered_df = joined_df \
    .filter(col("city").like("a%") | col("city").like("%-%"))

rental_time_df = filtered_df \
    .withColumn("rental_duration_hours",
                (unix_timestamp(col("return_date")) - unix_timestamp(col("rental_date"))) / 3600) \
    .groupBy("name") \
    .agg(sum("rental_duration_hours").alias("total_hours"))

result_df = rental_time_df \
    .orderBy(col("total_hours").desc()) \
    .limit(1)

result_df.show()