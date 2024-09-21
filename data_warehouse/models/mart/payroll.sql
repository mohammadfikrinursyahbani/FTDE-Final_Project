{{
    config(
        materialized = "table",
        schema = "mart"
    )
}}

SELECT
    p."PayrollID",
    e."Name" AS EmployeeName,
    p."PaymentDate",
    p."Salary",
    p."OvertimePay"
FROM {{ source('public', 'payroll') }} AS p
JOIN {{ source('public', 'employee') }} AS e ON e."EmployeeID" = p."EmployeeID"