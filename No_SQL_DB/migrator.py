
import psycopg2
import json
from pymongo import MongoClient
from urllib.parse import urlparse


pg_url = "postgresql://user1:123321@localhost:5432/new_lr1_dat"


url_parts = urlparse(pg_url)
pg_host = url_parts.hostname
pg_port = url_parts.port
pg_database = url_parts.path[1:]
pg_user = url_parts.username
pg_password = url_parts.password


mongo_host = 'localhost'
mongo_port = 27017
mongo_database = 'lr_5_data'


tables = ['student', 'student_balls', 'student_location']

def export_postgresql_data_to_json(tables : list):
    pg_url = "postgresql://user1:123321@localhost:5432/new_lr1_dat"
    url_parts = urlparse(pg_url)
    pg_host = url_parts.hostname
    pg_port = url_parts.port
    pg_database = url_parts.path[1:]
    pg_user = url_parts.username
    pg_password = url_parts.password

    mongo_host = 'localhost'
    mongo_port = 27017
    mongo_database = 'lr_5_data'

    def export_postgresql_data_to_json(tables):
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password
        )

        try:
            pg_cursor = pg_conn.cursor()

            for table in tables:
                pg_cursor.execute(f'SELECT * FROM {table}')
                records = pg_cursor.fetchall()

                if not records:
                    print(f"No data found in table {table}")
                else:
                    json_filename = f'{table}.json'
                    with open(json_filename, 'w') as json_file:
                        json.dump(records, json_file, default=str)
                    print(f'Data from table {table} exported to {json_filename}')

            pg_cursor.close()
        except Exception as e:
            print(f"Error during export: {str(e)}")
        finally:
            pg_conn.close()
            print("Operation completed.")

def import_json_to_mongodb(json_files, mongo_database, mongo_collections):

    mongo_host = 'localhost'
    mongo_port = 27017

    mongo_client = MongoClient(mongo_host, mongo_port)
    mongo_db = mongo_client[mongo_database]
    i = 0
    try:
        for json_file in json_files:
            # Read JSON data
            with open(json_file, 'r') as file:
                data = json.load(file)

            if not data:
                print(f"No data found in {json_file}")
            else:
                # Convert each record to a dictionary with named keys
                data_dicts = [
                    {
                        'uuid': record[0],
                        'year': record[1],
                        'gender': record[2],
                        'status': record[3],
                        'education_type': record[4]
                    }
                    for record in data
                ]

                # Insert data into MongoDB collection
                mongo_db[mongo_collections[i]].insert_many(data_dicts)
                print(f'Data from {json_file} imported into MongoDB collection {mongo_collections[i]}')
            i+=1
    except Exception as e:
        print(f"Error during import: {str(e)}")
    finally:
        mongo_client.close()
        print("Import operation completed.")



