{{
    config(
        materialized = "table",
        schema = "mart"
    )
}}

SELECT *
FROM {{ source('public', 'employee') }}