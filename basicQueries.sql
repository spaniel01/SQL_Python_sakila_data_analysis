### Movies and genre

# Breakdown, movies by genre
SELECT name, COUNT(*) AS N, 
ROUND(COUNT(*)/(SELECT COUNT(*) FROM FILM)*100, 1) AS Percentage 
FROM film
LEFT JOIN film_category USING(film_id)
LEFT JOIN category USING(category_id)
GROUP BY name 
ORDER BY N DESC; 

# Breakdown, movies by rate 
SELECT rental_rate, COUNT(*) AS freq_movies FROM film
GROUP BY rental_rate 
ORDER BY rental_rate;

# Breakdown, movies by genre and rate 
SELECT rental_rate, name AS genre, COUNT(*) AS freq_movies 
FROM film LEFT JOIN film_category USING(film_id) 
LEFT JOIN category USING(category_id)
GROUP BY rental_rate, genre ORDER BY genre, rental_rate DESC;

### Movie popularity

#Movie popularity (times rented), ranked
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
ORDER BY N_rented DESC) AS pop, 
(SELECT @_rank := 0, @rankval := 0) AS x;

### Actor popularity

# N of actor roles in films and actor popularity based on N of rented movies with that actor, ranked
WITH roles_tab AS(
SELECT devTab.*, @_rank := CASE
	WHEN @rankval = roles THEN @_rank
    WHEN @rankval <> roles AND (@rankval := roles) THEN @_rank + 1
    END AS ranking_act_roles 
FROM (
SELECT CONCAT(a.last_name, " ", a.first_name) AS full_name, COUNT(*) AS roles
FROM film
LEFT JOIN film_actor AS f USING(film_id)
LEFT JOIN actor AS a USING(actor_id)
GROUP BY CONCAT(a.last_name, " ", a.first_name) 
ORDER BY roles DESC) AS devTab,
(SELECT @_rank := 0, @rankval := 0) AS x),
popularity_tab AS( 
SELECT devTab2.*, @_rank2 := CASE
	WHEN @rankval2 = N_appearances_rented_movs THEN @_rank2
    WHEN @rankval2 <> N_appearances_rented_movs AND (@rankval2 := N_appearances_rented_movs) THEN @_rank2 + 1
    END AS ranking_N_appearances_rented_movs
FROM (
SELECT CONCAT(a.last_name, " ", a.first_name) AS full_name, COUNT(*) AS N_appearances_rented_movs FROM film 
LEFT JOIN film_actor USING(film_id)
LEFT JOIN actor a USING(actor_id)
LEFT JOIN inventory USING(film_id)
RIGHT JOIN rental USING(inventory_id)
GROUP BY full_name
ORDER BY N_appearances_rented_movs DESC) AS devTab2,
(SELECT @_rank2 := 0, @rankval2 := 0) AS x)
SELECT full_name, roles, ranking_act_roles, N_appearances_rented_movs, ranking_N_appearances_rented_movs,
((ranking_act_roles + ranking_N_appearances_rented_movs)/2) AS avg_rank FROM roles_tab 
LEFT JOIN  popularity_tab USING(full_name)
WHERE full_name IS NOT NULL
ORDER BY avg_rank;

### Performance and earnings

# N of rented movies by month 
WITH movied_by_month AS (SELECT CONCAT(YEAR(rental_date), ".", MONTH(rental_date)) AS distinct_dates, COUNT(*) AS N_rentals
FROM rental 
GROUP BY CONCAT(YEAR(rental_date), ".", MONTH(rental_date))),
cumsum_earning_by_month AS (SELECT *, 
SUM(monthlySUM) OVER(
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumSum
FROM (
SELECT CONCAT(YEAR(payment_date), ".", MONTH(payment_date)) AS distinct_dates, SUM(amount) AS monthlySum
FROM payment
GROUP BY MONTH(payment_date)) AS subQ)
SELECT * FROM movied_by_month 
LEFT JOIN cumsum_earning_by_month USING(distinct_dates);

### Customers, new and spending

# New customers by store and month
WITH customer_join_date AS (
SELECT store_id, customer_id, min(payment_date) AS date_joined_approx,
CAST(CONCAT(YEAR(min(payment_date)), ".", MONTH(min(payment_date))) AS DOUBLE) AS date_year_month
FROM store 
LEFT JOIN customer USING(store_id) 
LEFT JOIN payment USING(customer_id)
GROUP BY customer_id),
customers_cum_monthly AS (
SELECT store_id, COUNT(*) AS new_cust_monthly, date_year_month 
FROM customer_join_date 
GROUP BY store_id, date_year_month)
SELECT store_id, date_year_month, new_cust_monthly, 
SUM(new_cust_monthly) OVER(
PARTITION BY store_id 
ORDER BY store_id, date_year_month 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_new_cust
FROM customers_cum_monthly;

# Best customers, total
SELECT customer_id, last_name, SUM(amount) AS total_spent 
FROM payment 
LEFT JOIN customer USING (customer_id) 
GROUP BY customer_id 
ORDER BY total_spent DESC;

# Best customers, total and members since...
WITH customerTotalSpend AS (
SELECT customer_id, last_name, 
SUM(amount) AS total_spent 
FROM payment 
LEFT JOIN customer USING (customer_id) 
GROUP BY customer_id 
ORDER BY total_spent DESC
),
customerFirstTakeout AS (
SELECT customer_id, MIN(payment_date) AS first_takeout 
FROM payment
GROUP BY customer_id) 
SELECT * FROM customerTotalSpend 
LEFT JOIN customerFirstTakeout USING(customer_id);

### Customers, location

# Overview, store and customer addresses
WITH storeLocation AS(
SELECT store_id, city, district, address
FROM store 
LEFT JOIN address USING(address_id)
LEFT JOIN city USING(city_id)
LEFT JOIN country USING (country_id)
),
customerLocation AS (
SELECT customer_id, store_id, last_name, city AS customer_city, district AS customer_district, address AS customer_address 
FROM customer 
LEFT JOIN address USING (address_id)
LEFT JOIN city USING (city_id)
)
SELECT * FROM storeLocation 
LEFT JOIN customerLocation USING(store_id);

# Aggregation of customers by store and city district
WITH customers_by_district AS(
SELECT store_id, COUNT(*) AS customer_district_size, district 
FROM store 
LEFT JOIN customer USING(store_id) 
LEFT JOIN address ON address.address_id = customer.address_id 
GROUP BY store_id, district 
ORDER BY store_id, customer_district_size DESC)
SELECT store_id, customer_district_size, 
COUNT(*) AS customer_dist_freq, 
customer_district_size*COUNT(*) AS total_customers_by_district_size 
FROM customers_by_district 
GROUP BY store_id, customer_district_size 
ORDER BY store_id, total_customers_by_district_size DESC;

### Movie availability

#Movies currently rented out
SELECT title, rental_date, return_date, customer_id, last_name 
FROM inventory 
LEFT JOIN film USING(film_id) 
LEFT JOIN rental USING(inventory_id) 
LEFT JOIN customer USING(customer_id)
WHERE return_date IS NULL AND rental_date IS NOT NULL;

#Movies currently available
SELECT title, category.name AS genre, MAX(return_date) AS last_rented_until, customer_id 
FROM inventory LEFT JOIN film USING(film_id) 
LEFT JOIN film_category USING(film_id) 
LEFT JOIN category USING(category_id) 
LEFT JOIN rental USING(inventory_id) 
LEFT JOIN customer USING(customer_id)
WHERE return_date IS NOT NULL
GROUP BY title;

#Movies returned but not paid
SELECT film_id, title, customer.customer_id, CONCAT(last_name, " ", first_name) AS full_name, email
FROM payment 
LEFT JOIN customer USING(customer_id) 
LEFT JOIN rental USING(rental_id) 
LEFT JOIN inventory USING(inventory_id) 
LEFT JOIN film USING(film_id)
WHERE return_date IS NOT NULL AND payment_date IS NULL;
# Check if correct: Yes
SELECT *
FROM payment 
WHERE payment_date IS NULL;

#Best earning rented movies
SELECT film_id, title, rental_rate, COUNT(rental_date) AS N_takeout, SUM(amount) AS total_earnings_by_movie, 
CONCAT(YEAR(MIN(payment_date)), ".", MONTH(MIN(payment_date))) AS in_stock_since_approx
FROM payment 
LEFT JOIN customer USING(customer_id) 
LEFT JOIN rental USING(rental_id) 
LEFT JOIN inventory USING(inventory_id) 
LEFT JOIN film USING(film_id)
GROUP BY film_id
ORDER BY total_earnings_by_movie DESC;

# Movie earning stats as function of time in stock
WITH total_earnings_by_mov AS(SELECT film_id, title, rental_rate, COUNT(rental_date) AS N_takeout, SUM(amount) AS total_earnings_by_movie, 
CONCAT(YEAR(MIN(payment_date)), ".", MONTH(MIN(payment_date))) AS in_stock_since_approx
FROM payment 
LEFT JOIN customer USING(customer_id) 
LEFT JOIN rental USING(rental_id) 
LEFT JOIN inventory USING(inventory_id) 
LEFT JOIN film USING(film_id)
GROUP BY film_id
ORDER BY total_earnings_by_movie DESC)
SELECT in_stock_since_approx, 
ROUND(AVG(total_earnings_by_movie),1) AS average_total_mov_earning, 
ROUND(STD(total_earnings_by_movie),2) AS stand_dev_total_mov_earning 
FROM total_earnings_by_mov
GROUP BY in_stock_since_approx;

### Views

# Movies currently unavailable
CREATE VIEW movies_available AS
SELECT title, rental_date, return_date, customer_id, last_name 
FROM inventory 
LEFT JOIN film USING(film_id) 
LEFT JOIN rental USING(inventory_id) 
LEFT JOIN customer USING(customer_id)
WHERE return_date IS NULL AND rental_date IS NOT NULL;

# Use view example, careful, must be updated!
SELECT * FROM movies_available;
SELECT * FROM movies_available WHERE title = "HUNGER ROOF";

#Movies currently available
CREATE VIEW movies_unavailable AS
SELECT title, category.name AS genre, MAX(return_date) AS last_rented_until, customer_id 
FROM inventory LEFT JOIN film USING(film_id) 
LEFT JOIN film_category USING(film_id) 
LEFT JOIN category USING(category_id) 
LEFT JOIN rental USING(inventory_id) 
LEFT JOIN customer USING(customer_id)
WHERE return_date IS NOT NULL
GROUP BY title;

### Database problems

# Distinct dates (gap between 2005.8, when series ends, and 2006.2, last entries) in rental (and payment)
SELECT DISTINCT CONCAT(YEAR(rental_date), " ", MONTH(rental_date)) AS distinct_dates, COUNT(*) AS N_entries
FROM rental
LEFT JOIN inventory USING(inventory_id)
LEFT JOIN film USING(film_id) 
GROUP BY distinct_dates;

# Viewing potentially problematic date based data entries (from directly above)
SELECT *
FROM rental
LEFT JOIN inventory USING(inventory_id)
LEFT JOIN film USING(film_id) 
WHERE rental_date > "2005-09-01";
# It is noticeable that return_date is NULL for all these. 
# Perhaps a system error that assigns wrong date during movie checkout but enters correct dates when returned?

# Movies with no actors 
SELECT * FROM film 
LEFT JOIN film_actor USING(film_id)
LEFT JOIN actor USING(actor_id)
WHERE last_name is NULL;
# Based on movie description, each of these movies features a human actor. Must be added

# Film by language
SELECT name, COUNT(*) FROM film
LEFT JOIN language USING(language_id)
GROUP BY name;
# Only English, likely to be error, should be fixed

# Acadmey Dinosaur, odd entry: has inventory_id, has been rented before, but no rental_id etc. 
SELECT * FROM film 
LEFT JOIN inventory USING(film_id)
LEFT JOIN rental USING(inventory_id)
WHERE rental_date is NULL AND title = "ACADEMY DINOSAUR";
