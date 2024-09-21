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
    (p."FinishTraining" - p."StartTraining") AS TrainingDuration
FROM {{ source('public', 'training') }} AS t
JOIN {{ source('public', 'payroll') }} AS p ON t."TrainingID" = p."TrainingID"