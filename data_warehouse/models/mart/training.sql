{{
    config(
        materialized = "table",
        schema = "mart"
    )
}}

SELECT
    t."TrainingID",
    p."EmployeeID",
    t."TrainingProgram",
    (p."FinishTraining" - p."StartTraining") AS TrainingDuration,
    p."StatusTraining"
FROM {{ source('public', 'payroll') }} AS p
LEFT JOIN {{ source('public', 'training') }} AS t ON t."TrainingID" = p."TrainingID"