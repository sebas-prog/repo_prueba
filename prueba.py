import pandas as pd
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import pyodbc
import sqlalchemy
import pyodbc
import os

def prueba_1():
 
    data = {
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
            'Age': [24, 30, 35, 45, 28],
            'City': ['NY', 'LA', 'Chicago', 'Houston', 'Phoenix']
        }

    df = pd.DataFrame(data)

    server = "LAPTOP-PQNI4F62\SQLEXPRESS"
    database= "TRABAJO BI"

    conn =  'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con)

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True

    table_name = 'data_1'
    df.to_sql(table_name, engine, if_exists = 'append',index=False)

def prueba_2():
 
    data = {
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
            'Age': [24, 30, 35, 45, 28],
            'City': ['NY', 'LA', 'Chicago', 'Houston', 'Phoenix']
        }

    df = pd.DataFrame(data)

    server = "LAPTOP-PQNI4F62\SQLEXPRESS"
    database= "TRABAJO BI"

    conn =  'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con)

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True

    table_name = 'data_2'
    df.to_sql(table_name, engine, if_exists = 'append',index=False)

def prueba_3():
 
    data = {
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
            'Age': [24, 30, 35, 45, 28],
            'City': ['NY', 'LA', 'Chicago', 'Houston', 'Phoenix']
        }

    df = pd.DataFrame(data)

    server = "LAPTOP-PQNI4F62\SQLEXPRESS"
    database= "TRABAJO BI"

    conn =  'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con)

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True

    table_name = 'data_3'
    df.to_sql(table_name, engine, if_exists = 'append',index=False)

def prueba_4():
 
    data = {
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
            'Age': [24, 30, 35, 45, 28],
            'City': ['NY', 'LA', 'Chicago', 'Houston', 'Phoenix']
        }

    df = pd.DataFrame(data)

    server = "LAPTOP-PQNI4F62\SQLEXPRESS"
    database= "TRABAJO BI"

    conn =  'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con)

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True

    table_name = 'data_4'
    df.to_sql(table_name, engine, if_exists = 'append',index=False)

def delete_txt_files():
    directory = os.getcwd()
    # Itera sobre todos los archivos en el directorio
    for file_name in os.listdir(directory):
        # Si el archivo termina en '.txt', lo elimina
        if file_name.endswith('.txt'):
            file_path = os.path.join(directory, file_name)
            os.remove(file_path)
            print(f"Deleted {file_path}")