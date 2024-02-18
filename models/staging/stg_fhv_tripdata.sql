with

    source as (select * from {{ source("staging", "fhv_tripdata") }}),

    renamed as (

        select
            -- identifiers
            {{
                dbt_utils.generate_surrogate_key(
                    ["dispatching_base_num", "lpep_pickup_datetime"]
                )
            }} as tripid,
            dispatching_base_num,

            -- timestamps
            cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
            cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
            EXTRACT(year FROM cast(lpep_pickup_datetime as timestamp)) AS pickup_year,

            {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
            as pickup_locationid,
            {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
            as dropoff_locationid,

            sr_flag,
            affiliated_base_number

        from source

    ),
    filtered as (
        select * 
        from renamed 
        where pickup_year = 2019
        )

select *
from filtered

-- dbt build --select stg_fhv_tripdata.sql --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
