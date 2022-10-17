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
SELECT CONCAT(a.last_name, " ", a.first_name), COUNT(a.last_name) AS roles FROM film
LEFT JOIN film_actor AS f USING(film_id)
LEFT JOIN actor AS a USING(actor_id)
GROUP BY a.last_name 
ORDER BY roles DESC;

#Movie popularity
SELECT c.name, f.title, COUNT(*) AS N_rented FROM film f 
LEFT JOIN film_category f_c USING(film_id)
LEFT JOIN category c USING(category_id)
LEFT JOIN inventory USING(film_id)
RIGHT JOIN rental USING(inventory_id)
GROUP BY f.title
ORDER BY N_rented DESC;

#Actor popularity based on rented movies
SELECT CONCAT(a.last_name, " ", a.first_name) AS full_name, COUNT(*) AS N_appearances_rented_movs FROM film 
LEFT JOIN film_actor USING(film_id)
LEFT JOIN actor a USING(actor_id)
LEFT JOIN inventory USING(film_id)
RIGHT JOIN rental USING(inventory_id)
GROUP BY full_name
ORDER BY N_appearances_rented_movs DESC;

#Movies with no actors
SELECT * FROM film 
LEFT JOIN film_actor USING(film_id)
LEFT JOIN actor USING(actor_id)
WHERE last_name is NULL;


