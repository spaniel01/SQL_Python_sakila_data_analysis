# -*- coding: utf-8 -*-
import sqlalchemy as db
import pandas as pd

# Comment: As an excercise, this file contains the translation of the most relevant MySQL queries found in the same repository into SQLAlchemy core queries. To reduce clutter, the code to execute each statement was removed:
# connection.execute(stat).fetchall()
# Also, data was not yet transformed to pd.DataFrame() objects. This will be done directly in the Jupyter Notebook report, where these queries will be used to obtain DB data. This data will then be further analyzed and discussed. 
# NOTE 1: Sensitive data was removed from connection string! 
# NOTE 2: To be able to execute code within the Spyder (!) while providing for greater readability, "\" type line breaks were added .

# Create engine, load metadata, establish connection
engine = db.create_engine('mysql+mysqlconnector://***') 
metadata = db.MetaData()
metadata.reflect(bind=engine)
table_name_list = engine.table_names()
for i in table_name_list:
    str_name = i
    locals()[str_name] = db.Table(i, metadata, autoload=True, autoload_with=engine)
connection = engine.connect()

# 1. Breakdown, movies by genre
stat = db.select(category.c.name, 
                 db.func.count(film.c.title).label("N"),
                 (db.func.count(film.c.title)/(db.select(db.func.count(film.c.title)))).label("Percentage")
                 ).select_from(film.join(film_category)).join(category).group_by(category.c.name).order_by(db.desc("N"))

#2. Breakdown, movies by rate
stat = db.select(film.c.rental_rate, db.func.count()).\
    group_by(film.c.rental_rate).\
        order_by(film.c.rental_rate)

# 3. Breakdown movies by genre and rate 
stat = db.select(film.c.rental_rate, 
                 category.c.name.label("genre"),
                 db.func.count().label("freq_movies")).\
    select_from(film.join(film_category)).join(category).\
        group_by(film.c.rental_rate, "genre")\
            .order_by("genre", film.c.rental_rate.desc())

# 4. Movie popularity (no ranking)
stat = db.select(category.c.name, 
                 film.c.title,
                 db.func.count().label("N_rented")).\
    select_from(film.join(film_category)).\
        join(category).\
            join(inventory).\
                join(rental).\
                    group_by(film.c.title).\
                        order_by(db.desc("N_rented"))

# 5. Actor popularity (no ranking)
# Part 1
roles_tab_temp = db.select(
    db.func.concat(actor.c.last_name, " ", actor.c.first_name).label("full_name"),
    db.func.count().label("roles")).\
    select_from(film.join(film_actor)).\
        join(actor).\
            group_by("full_name").\
                order_by(db.desc("roles")).\
                    cte("roles_tab_temp")
# Part 2
appearances_tab_temp = db.select(
    db.func.concat(actor.c.last_name, " ", actor.c.first_name).label("full_name"),
    db.func.count().label("N_appearances_rented_movs")).\
    select_from(film.join(film_actor)).\
        join(actor).join(inventory).\
            join(rental).group_by("full_name").\
                order_by(db.desc("N_appearances_rented_movs")).\
                    cte("appearances_tab_temp")
# Final query
stat = db.select(
    roles_tab_temp.c.full_name, 
    roles_tab_temp.c.roles, 
    appearances_tab_temp.c.N_appearances_rented_movs) 

# 6. N of rented movies by month
# Part 1
movie_by_month_temp = db.select(
    db.func.concat(db.extract("year", rental.c.rental_date), ".", db.extract( "month",rental.c.rental_date)).label("distinct_dates"),
    db.func.count().label("N_rentals")).\
    group_by(db.func.concat(db.extract("year", rental.c.rental_date), ".", db.extract("month",rental.c.rental_date))).\
        cte("movie_by_month")
# Part 2
## Subquery
subQ =db.select(\
                db.func.concat(db.extract("year", rental.c.rental_date), ".", db.extract( "month",rental.c.rental_date)).label("distinct_dates"),
                db.func.sum(payment.c.amount).label("monthlySum")).\
    select_from(rental.join(payment)).\
        group_by(db.func.concat(db.extract("year", rental.c.rental_date), ".", db.extract( "month",rental.c.rental_date)))\
            .subquery("subQ")
## CTE
cumSumEarningByMonth = db.select(
    subQ.c.distinct_dates, 
    db.func.sum(subQ.c.monthlySum).over(rows = (None, 0)).label("cumsum")).\
    select_from(subQ).\
        cte("cumSumEarningByMonth")
# Final query
stat = db.select(
    movie_by_month_temp.c.distinct_dates, 
    movie_by_month_temp.c.N_rentals, 
    cumSumEarningByMonth.c.cumsum ).\
    select_from(movie_by_month_temp.join(\
                                         cumSumEarningByMonth, movie_by_month_temp.c.distinct_dates == cumSumEarningByMonth.c.distinct_dates))

# 7. Customers, new and spending
# Part 1
customer_join_date = db.select(
    store.c.store_id, customer.c.customer_id, 
    db.func.min(payment.c.payment_date).label("date_joined_approx"),
    db.cast(db.func.concat(db.extract("year", db.func.min(payment.c.payment_date)), ".", db.extract("month", db.func.min(payment.c.payment_date))),db.Float).label("date_year_month")).\
    select_from(store.join(customer))\
        .join(payment).\
            group_by(customer.c.customer_id)
# Part 2
customers_cum_monthly = db.select(
    customer_join_date.c.store_id, db.func.count().label("new_cust_monthly"),
    customer_join_date.c.date_year_month).\
    group_by(customer_join_date.c.store_id,
             customer_join_date.c.date_year_month).\
        cte("customers_cum_monthly")
# Final query
stat = db.select(
    customers_cum_monthly.c.store_id, 
    customers_cum_monthly.c.date_year_month, 
    customers_cum_monthly.c.new_cust_monthly,
    db.func.sum(customers_cum_monthly.c.new_cust_monthly).over(
        partition_by = customers_cum_monthly.c.store_id, 
        order_by = customers_cum_monthly.c.date_year_month, 
        rows = (None, 0)).label("cum_new_cust"))

# 8. Best customers
stat = db.select(
    customer.c.customer_id, 
    customer.c.last_name,
    db.func.sum(payment.c.amount).label("total_spent")]).\
        join(payment).\
            group_by(customer.c.customer_id).\
                order_by(db.desc("total_spent"))

# 9. Best customers, total and member since...
# Part 1
customerTotalSpend = db.select(
    customer.c.customer_id, 
    customer.c.last_name,
    db.func.sum(payment.c.amount).label("total_spent")).\
    select_from(payment.join(customer)).\
        group_by(customer.c.customer_id).\
            order_by(db.desc("total_spent")).\
                cte("customerTotalSpend")
# Part 2
customerFirstTakeOut = db.select(
    customer.c.customer_id,
    db.func.min(payment.c.payment_date).label("first_takeout")).\
    group_by(customer.c.customer_id).\
        cte("customerFirstTakeOut")
# Final query
stat = db.select(
    customerTotalSpend.\
        join(customerFirstTakeOut, 
             customerTotalSpend.c.customer_id == customerFirstTakeOut.c.customer_id))

# 10. Overview, store and customer addresses
# Part 1
storeLocation = db.select(
    store.c.store_id, 
    city.c.city, 
    address.c.district, 
    address.c.address).\
    select_from(store.join(address)).\
        join(city).\
            cte("storeLocation")
# Part 2
customerLocation =db.select(
    customer.c.customer_id, 
    customer.c.store_id, 
    customer.c.last_name, 
    city.c.city.label("customer_city"),
    address.c.address.label("customer_address")).\
    select_from(customer.join(address)).\
        join(city).\
            cte("customerLocation")
#Final query
stat = db.select(
    storeLocation.\
        join(customerLocation, 
             storeLocation.c.store_id == customerLocation.c.store_id))

# 11. Agg of customers by store and city district
# Part 1
customers_by_district = db.select(
    store.c.store_id, db.func.count().label("customer_district_size"),
    address.c.district).\
    select_from(store.join(customer)).\
        join(address).\
            group_by(store.c.store_id, address.c.district).\
                order_by(store.c.store_id, 
                         db.desc("customer_district_size")).\
                    cte("customers_by_district")
# Final Query
stat = db.select(
    customers_by_district.c.store_id, 
    customers_by_district.c.customer_district_size, 
    db.func.count().label("customer_dist_freq"),
    (customers_by_district.c.customer_district_size*db.func.count()).label("total_customers_by_district_size")).\
    group_by(customers_by_district.c.store_id,
             customers_by_district.c.customer_district_size).\
        order_by(customers_by_district.c.store_id, 
                 db.desc("total_customers_by_district_size"))

# 12. Movies currently rented out
stat = db.select(
    film.c.title, rental.c.rental_date, 
    rental.c.return_date, 
    customer.c.customer_id,
    customer.c.last_name).\
    select_from(inventory.\
                join(film).\
                    join(rental).\
                        join(customer)).\
        where(db.and_(rental.c.return_date == None), 
              rental.c.rental_date.isnot(None))

# 13. Movies currently available
stat = db.select(film.c.title, 
                 category.c.name.label("genre"), 
                 db.func.max(rental.c.return_date).label("last_rented_until"),
                 customer.c.customer_id).\
    select_from(inventory.\
                join(film).\
                    join(film_category).\
                        join(category).\
                            join(rental).\
                                join(customer)).\
        where(rental.c.return_date.isnot(None)).\
            group_by(film.c.title)

# 14. Movies returned but not paid: skipped, since result of query () was 0 rows

#15. Best earning rented movies
stat = db.select(
    film.c.film_id, 
    film.c.title, 
    film.c.rental_rate, 
    db.func.count(rental.c.rental_date).label("N_takeout"),
    db.func.sum(payment.c.amount).label("total_earnings_by_movie"), 
    db.func.concat(db.extract("year", db.func.min(payment.c.payment_date)), ".", db.extract( "month", db.func.min(payment.c.payment_date)).label("in_stock_since_approx"))).\
    select_from(payment.\
                join(customer).\
                    join(rental).\
                        join(inventory).\
                            join(film)).\
        group_by(film.c.film_id).\
            order_by(db.desc("total_earnings_by_movie"))

# 16. Movie earning stats as function of time in stock
#part1
earnings_mov_temp = db.select(
    film.c.film_id, 
    film.c.title, 
    film.c.rental_rate, 
    db.func.count(rental.c.rental_date).label("N_takeout"),
    db.func.sum(payment.c.amount).label("total_earnings_by_movie"), 
    db.func.concat(db.extract("year", db.func.min(rental.c.rental_date)), ".", db.extract( "month", db.func.min(rental.c.rental_date))).label("in_stock_since_approx")).\
    select_from(payment.join(customer).\
                join(rental).\
                    join(inventory).\
                        join(film)).\
        group_by(film.c.film_id).\
            order_by(db.desc("total_earnings_by_movie")).\
                cte("earnings_mov_temp")
#Final query
stat = db.select(
    earnings_mov_temp.c.in_stock_since_approx,
    db.func.round(db.func.avg(earnings_mov_temp.c.total_earnings_by_movie), 1).label("average_total_mov_earning"),
    db.func.round(db.func.stddev(earnings_mov_temp.c.total_earnings_by_movie), 2).label("stand_dev_total_mov_earning")).\
    group_by(earnings_mov_temp.c.in_stock_since_approx)

