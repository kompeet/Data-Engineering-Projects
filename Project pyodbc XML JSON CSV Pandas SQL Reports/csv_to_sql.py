import pyodbc
import pandas

try:
    connect = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=TestExercises;"
        "Trusted_Connection=yes;"
    )
    print("CONNECTED")
    cursor = connect.cursor()

    urlCSV = "https://raw.githubusercontent.com/green-fox-academy/teaching-materials/master/workshop/psycopg/employees/employees.csv?token=GHSAT0AAAAAAB3DLQOAGEE5YCQXDUGYOJG6Y3VIMKA"

    fileCSV = pandas.read_csv(urlCSV)
    readCSV = pandas.DataFrame(fileCSV)
    print(readCSV)

    for row in readCSV.itertuples():
        print(row)

        f_name = row[2]
        l_name = row[3]
        b_date = pandas.to_datetime(row[4].strip(), format="%m/%d/%Y")
        gender = row[5]
        salary = row[6]
        cursor.execute(
            '''
            INSERT INTO employee(first_name, last_name, branch, position, birth_date, gender, nationality, university, monthly_salary, salary_by_year) VALUES (?,?,?,?,?,?,?,?,?,?)
            ''',
            f_name,
            l_name,
            None,
            None,
            b_date,
            gender,
            None,
            None,
            salary,
            salary * 12
        )

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")


except pyodbc.ProgrammingError as e:
    print(e)
    print('Something wrong with SQL statement')
except pyodbc.OperationalError as e:
    print('SQL connection error')