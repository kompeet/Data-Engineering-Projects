USE Employees

--SELECT * FROM Employees.dbo.employee

--Which first name is the most common in the company?
SELECT first_name AS most_common_first_name, COUNT(first_name) AS [count]
FROM Employees.dbo.employee
GROUP BY first_name
ORDER BY [count] DESC
--Jay(2), Courtney(2), Amos(2), Collen(2) 

--Which first name is the most common among the younger (<30) employees?
SELECT first_name AS most_common_first_name, COUNT(first_name) AS [count]
FROM Employees.dbo.employee
WHERE birth_date < '1992-11-01'
GROUP BY first_name
ORDER BY [count] DESC
--Amos(2), Jay(2)

--What is the median salary in the company?
SELECT
(
 (SELECT MAX(monthly_salary) FROM
   (SELECT TOP 50 PERCENT monthly_salary FROM Employees.dbo.employee ORDER BY monthly_salary) AS BottomHalf)
 +
 (SELECT MIN(monthly_salary) FROM
   (SELECT TOP 50 PERCENT monthly_salary FROM Employees.dbo.employee ORDER BY monthly_salary DESC) AS TopHalf)
) / 2 AS Median
--median monthly salary is 6371

--How many employee earns more than the average? 
SELECT SUM(CASE WHEN monthly_salary > AVG(monthly_salary) then 1 else 0 END) AS count_more_than_avg
FROM Employees.dbo.employee
