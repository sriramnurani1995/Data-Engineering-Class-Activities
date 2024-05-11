# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
TableName = 'censusdata'
Datafile = "AL2015_1.csv"  
CreateDB = False  




def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable


# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE TABLE {TableName} (
				CensusTract         NUMERIC,
				State               TEXT,
				County              TEXT,
				TotalPop            INTEGER,
				Men                 INTEGER,
				Women               INTEGER,
				Hispanic            DECIMAL,
				White               DECIMAL,
				Black               DECIMAL,
				Native              DECIMAL,
				Asian               DECIMAL,
				Pacific             DECIMAL,
				Citizen             DECIMAL,
				Income              DECIMAL,
				IncomeErr           DECIMAL,
				IncomePerCap        DECIMAL,
				IncomePerCapErr     DECIMAL,
				Poverty             DECIMAL,
				ChildPoverty        DECIMAL,
				Professional        DECIMAL,
				Service             DECIMAL,
				Office              DECIMAL,
				Construction        DECIMAL,
				Production          DECIMAL,
				Drive               DECIMAL,
				Carpool             DECIMAL,
				Transit             DECIMAL,
				Walk                DECIMAL,
				OtherTransp         DECIMAL,
				WorkAtHome          DECIMAL,
				MeanCommute         DECIMAL,
				Employed            INTEGER,
				PrivateWork         DECIMAL,
				PublicWork          DECIMAL,
				SelfEmployed        DECIMAL,
				FamilyWork          DECIMAL,
				Unemployment        DECIMAL
			);	
		""")
		print(f"Created {TableName}")


  
def load_data(conn, datafile):
    with conn.cursor() as cursor, open(datafile, 'r') as f:
        start = time.perf_counter()
        next(f)  
        cursor.copy_from(f,TableName, sep=',',null='')  
        print("Data loaded using COPY.")
        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
        
        
def check_table_exists(conn):
    with conn.cursor() as cursor:
        # Execute the query to check if the 'CensusData' table exists
        cursor.execute("SELECT 1 FROM pg_tables WHERE tablename = 'CensusData';")
        result = cursor.fetchall()  # Fetch all results
        if result:
            print("Table 'CensusData' exists in the database.")
            for row in result:
                print(row)
        else:
            print("Table 'CensusData' does not exist in the database.")


def main():
    initialize()
    print(Datafile)
    conn = dbconnect()
    if CreateDB:
        createTable(conn)
    load_data(conn, Datafile)



if __name__ == "__main__":
	main()



