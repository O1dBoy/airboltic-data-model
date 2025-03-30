{% docs hub__orders %}

# hub__orders

This model sources the data from this landing table (
    `landing.landing__orders`
)

It contains set of keys which has low propensity to change such as:

- hub__order__nk (order_id)
- hub__order__key (SHA1 of order_id)

The events in this raw_data_vault table are recorded (_load_datetime) by
first ingestion to the raw vault.
The table is incremental, only the first record (based on the nk)
that appeared in the source is loaded to the hub.

{% enddocs %}
