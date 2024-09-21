{{
    config(
        materialized = "table",
        schema = "mart"
    )
}}

SELECT
    r."CandidateID",
    r."ApplicationDate",
    r."InterviewDate",
    r."Status",
    r."OfferStatus",
    r."Prediction"
FROM {{ source('public','recruitment') }} AS r