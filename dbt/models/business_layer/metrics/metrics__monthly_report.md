{% docs metrics__monthly_report %}

This SQL model calculates monthly metrics for orders and trips from the `int__orders_info_enriched` table. 

### Metrics Calculated:
1. `trip_month`: Extracted month from the `trip_end_date`.
2. `orders_count`: Count of distinct orders.
3. `total_revenue_eur`: Total revenue in euros.
4. `avg_spent_eur`: Average amount spent per order in euros.
5. `trips_count`: Count of distinct trips.
6. `total_trips_duration_hours`: Total duration of all trips in hours.
7. `avg_trip_duration_hours`: Average duration of trips in hours.
8. `active_customers`: Count of distinct active customers.

{% enddocs %}