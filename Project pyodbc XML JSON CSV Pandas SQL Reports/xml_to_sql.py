import xml.etree.ElementTree as et

import pandas
import pyodbc
from lxml import etree

try:
    connect = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=TestExercises;"
        "Trusted_Connection=yes;"
    )
    cursor = connect.cursor()
    print('CONNECTED')

    with open('employees.xml', 'r', encoding='utf-8') as fileXML:
        dictXML = et.parse(fileXML)
        rootXML = dictXML.getroot()

        cols = ['first_name', 'last_name', 'birth_date',
                'branch', 'gender', 'salary_by_year', 'position']
        rows = []

        for node in rootXML:
            s_name = node.find('name').text if node is not None else None
            firstTag = s_name.split(' ')[0]
            lastTag = s_name.split(' ')[1]
            branchTag = node.find('branch').text if node is not None else None
            positionTag = node.find(
                'position').text if node is not None else None
            birthTag = node.find(
                'birth_date').text if node is not None else None
            genderTag = node.find('gender').text if node is not None else None
            yearTag = node.find(
                'salary_by_year').text if node is not None else None

            rows.append({'first_name': firstTag, 'last_name': lastTag, 'birth_date': birthTag, 'branch': branchTag,
                         'gender': genderTag, 'salary_by_year': yearTag, 'position': positionTag})

        XMLdf = pandas.DataFrame(rows, columns=cols)
        XMLdf['birth_date'] = pandas.to_datetime(XMLdf['birth_date'])
        XMLdf['salary_by_year'] = pandas.to_numeric(
            XMLdf['salary_by_year'], downcast="integer")
        print(XMLdf)
        print(cols)
        print(rows)

        for i in XMLdf.itertuples():
            firstTag = i[1]
            lastTag = i[2]
            branchTag = i[4]
            positionTag = i[7]
            birthTag = i[3]
            genderTag = i[5]
            yearTag = i[6]
            print(type(yearTag))
            cursor.execute(
                "INSERT INTO employee(first_name, last_name, branch, position, birth_date, gender, nationality, university, monthly_salary, salary_by_year) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (firstTag, lastTag, branchTag, positionTag, birthTag, genderTag, None, None, yearTag / 12, yearTag))

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")


except pyodbc.ProgrammingError as e:
    print(e)
    print('Something wrong with SQL statement')
except pyodbc.OperationalError as e:
    print('SQL connection error')
