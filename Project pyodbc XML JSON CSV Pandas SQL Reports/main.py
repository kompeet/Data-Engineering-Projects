import csv
from datetime import date

import pyodbc
import pandas
import lxml
import xmltodict

try:
    connect = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=TestExercises;"
        "Trusted_Connection=yes;"
    )
    cursor = connect.cursor()

    cursor.execute(
        "CREATE TABLE employee(id int IDENTITY(1,1), first_name VARCHAR(50), last_name VARCHAR(50), branch VARCHAR(50), position VARCHAR(50), birth_date DATE, gender VARCHAR(10), nationality VARCHAR(20), university VARCHAR(100), monthly_salary int, salary_by_year int)"
     )

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")


except pyodbc.ProgrammingError as e:
    print(e)
except pyodbc.OperationalError as e:
    print(e)