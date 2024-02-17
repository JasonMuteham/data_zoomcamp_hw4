with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}