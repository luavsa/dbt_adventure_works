WITH
   employee_source AS (
        SELECT
            CAST(businessentityid AS int) AS pk_employee,
            CAST(nationalidnumber AS int) AS id_national_number,
            CAST(loginid AS varchar) AS id_login,
            CAST(jobtitle AS varchar) AS job_title,
            CAST(birthdate AS date) AS birth_date,
            CAST(maritalstatus AS varchar) AS maritial_status,
            CAST(gender AS varchar) AS gender,
            CAST(hiredate AS date) AS hire_date,
            CAST(salariedflag AS boolean) AS salaried_flag,
            CAST(vacationhours AS int) AS vacation_hours,
            CAST(sickleavehours AS int) AS sick_leave_hours,
            CAST(currentflag AS boolean) AS current_flag,
            CAST(rowguid AS varchar) AS employee_rowguid,
            CAST(modifieddate AS varchar) AS employee_modified_dt,
            CAST(organizationnode AS varchar) AS organization_node
        FROM {{ source("adventure_works", "employee") }}
    )

SELECT *
FROM employee_source