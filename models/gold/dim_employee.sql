WITH employee AS (
    SELECT *
    FROM {{ ref('stg_employee') }}
),

person AS (
    SELECT *
    FROM {{ ref('stg_person') }}
),

sales AS (
    SELECT *
    FROM {{ ref('stg_sales_person') }}
),

joined AS (
    SELECT
        employee.pk_employee,
        person.person_first_name || ' ' || person.person_middle_name || ' ' || person.person_last_name AS employee,
        employee.job_title,
        employee.birth_date,
        CASE WHEN employee.maritial_status = 'S' THEN 'Single' ELSE 'Married' END AS maritial_status,
        CASE WHEN employee.gender = 'F' THEN 'Female' ELSE 'Male' END AS gender,
        employee.hire_date,
        sales.sales_quota,
        sales.sales_ytd,
        sales.sales_last_year
    FROM employee
    LEFT JOIN person ON employee.pk_employee = person.pk_business_entity
    LEFT JOIN sales ON employee.pk_employee = sales.fk_business_entity
)

SELECT *
FROM joined