import luigi
import time
import pandas as pd
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import pyodbc
import sqlalchemy
import pyodbc
#import os
import os

def prueba_1():
 
    df = pd.read_excel(r'c:\Users\atita\Downloads\bd.xlsx')

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


class TaskA(luigi.Task):
    """
    Tarea A: Simula trabajo que lleva 2 segundos.
    """
    def run(self):
        print("Running Task A...")
        time.sleep(2)  # Simula trabajo de 2 segundos
        prueba_1()
        print("Task A is done.")

if __name__ == '__main__':
    luigi.build([TaskA()], local_scheduler=True)
    #prueba_1()
