{{ 
    config(
        materialized = 'table'
    ) 
}}


{% if env_var('ENV') != "prod" %}
    {% set aeroplane_models_events = "NHero_dev.aeroplane_models" %}
{% else %}
    {% set aeroplane_models_events = "abstraction_to_s3.aeroplane_models" %}
{% endif %}

select
    key,
    value,
    'aeroplane-models-kafka-topic' as topic,
    cast('20250201' as timestamp) as updated_at,
    20250301 as imported_at
from {{ aeroplane_models_events }}
