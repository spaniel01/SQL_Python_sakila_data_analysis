SELECT COUNT(*) FROM actor;
SELECT * FROM actor LIMIT 1;
SELECT COUNT(*) FROM film;
SELECT COUNT(*) FROM language GROUP BY language;
SELECT * FROM rental;
SELECT* FROM nicer_but_slower_film_list;
###################################################
# Film by language: only English
SELECT name, COUNT(*) FROM film
LEFT JOIN language USING(language_id)
GROUP BY name;

# Breakdown, movies by category
SELECT name, COUNT(*) AS N, 
ROUND(COUNT(*)/(SELECT COUNT(*) FROM FILM)*100, 1) AS Percentage 
FROM film
LEFT JOIN film_category USING(film_id)
LEFT JOIN category USING(category_id)
GROUP BY name 
ORDER BY N DESC; 

# Occurence of actors in films
SELECT CONCAT(a.last_name, " ", a.first_name) AS full_name, COUNT(*) AS roles FROM film
LEFT JOIN film_actor AS f USING(film_id)
LEFT JOIN actor AS a USING(actor_id)
GROUP BY CONCAT(a.last_name, " ", a.first_name) 
ORDER BY roles DESC;

#Movie popularity
SELECT c.name, f.title, COUNT(*) AS N_rented FROM film f 
LEFT JOIN film_category f_c USING(film_id)
LEFT JOIN category c USING(category_id)
LEFT JOIN inventory USING(film_id)
RIGHT JOIN rental USING(inventory_id)
GROUP BY f.title
ORDER BY N_rented DESC;

#Movie popularity, ranked
SELECT pop.*, @_rank := CASE
	WHEN @rankval = N_rented THEN @_rank
    WHEN @rankval <> N_rented AND (@rankval := N_rented) THEN @_rank + 1
    END AS ranking 
FROM (SELECT c.name, f.title, COUNT(*) AS N_rented FROM film f 
LEFT JOIN film_category f_c USING(film_id)
LEFT JOIN category c USING(category_id)
LEFT JOIN inventory USING(film_id)
RIGHT JOIN rental USING(inventory_id)
GROUP BY f.title
ORDER BY N_rented) AS pop, 
(SELECT @_rank := 0, @rankval := 0) AS x;

SELECT N_rented, @_rank := CASE
    WHEN @rankval = N_rented THEN @_rank
    WHEN @rankval <> N_rented AND (@rankval := N_rented) THEN @_rank + 1
    END AS ranking 
FROM table_2, 
(SELECT @_rank := 0, @rankval := 0) AS x;

#Actor popularity based on rented movies
SELECT CONCAT(a.last_name, " ", a.first_name) AS full_name, COUNT(*) AS N_appearances_rented_movs FROM film 
LEFT JOIN film_actor USING(film_id)
LEFT JOIN actor a USING(actor_id)
LEFT JOIN inventory USING(film_id)
RIGHT JOIN rental USING(inventory_id)
GROUP BY full_name
ORDER BY N_appearances_rented_movs DESC;

# Movies with no actors
SELECT * FROM film 
LEFT JOIN film_actor USING(film_id)
LEFT JOIN actor USING(actor_id)
WHERE last_name is NULL;

# Distinct dates in rental (same as payment)
SELECT DISTINCT CONCAT(YEAR(rental_date), " ", MONTH(rental_date)) AS distinct_dates
FROM rental
LEFT JOIN inventory USING(inventory_id)
LEFT JOIN film USING(film_id);

# N of rented movies by month with grouping
SELECT CONCAT(YEAR(rental_date), " ", MONTH(rental_date)) AS distinct_dates, COUNT(*) AS N_rentals
FROM rental 
GROUP BY CONCAT(YEAR(rental_date), " ", MONTH(rental_date));

# Payment cumulative by month 
SELECT payment_date, CONCAT(YEAR(payment_date), " ", MONTH(payment_date)) AS distinct_dates, 
SUM(amount) OVER(PARTITION BY CONCAT(YEAR(payment_date), " ", MONTH(payment_date))
ORDER BY payment_date
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumsum_monthly_income
FROM payment;

# Max monthly, min monthly 
SELECT CONCAT(YEAR(payment_date), " ", MONTH(payment_date)) AS distinct_dates, 
MAX(amount), MIN(amount)
FROM payment
GROUP BY CONCAT(YEAR(payment_date), " ", MONTH(payment_date));

# Monthly cumsum earnings
SELECT *, 
SUM(monthlySUM) OVER(
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumSum
FROM (SELECT CONCAT(YEAR(payment_date), " ", MONTH(payment_date)) AS distinct_dates, SUM(amount) AS monthlySum
FROM payment
GROUP BY MONTH(payment_date)) AS subQ;

# Best paying customers
SELECT customer_id, last_name, SUM(amount) AS total_spent FROM payment LEFT JOIN customer USING (customer_id) GROUP BY customer_id ORDER BY total_spent DESC;

# CTE customers to first rent
WITH customerTotalSpend AS (
SELECT customer_id, last_name, 
SUM(amount) AS total_spent 
FROM payment LEFT JOIN customer USING (customer_id) 
GROUP BY customer_id 
ORDER BY total_spent DESC
),
customerFirstTakeout AS (
SELECT customer_id, MIN(payment_date) AS first_takeout 
FROM payment
GROUP BY customer_id) 
SELECT * FROM customerTotalSpend 
LEFT JOIN customerFirstTakeout USING(customer_id);

SELECT customer_id, last_name, address
FROM customer LEFT JOIN address USING (address_id);

# Overview, store and customer addresses
WITH storeLocation AS(
SELECT store_id, city, district, address
FROM store LEFT JOIN address USING(address_id)
LEFT JOIN city USING(city_id)
LEFT JOIN country USING (country_id)
),
customerLocation AS (
SELECT customer_id, store_id, last_name, city AS customer_city, district AS customer_district, address AS customer_address 
FROM customer LEFT JOIN address USING (address_id)
LEFT JOIN city USING (city_id)
)
SELECT * FROM storeLocation 
LEFT JOIN customerLocation USING(store_id);

# Aggregate N_customers by store, aggrgate customers by store across time, count number of employees

# Create additional views, perhaps some from the above

# Create a procedure

#Check which customers have currently rented movies, which have rented/ returned but not paid

