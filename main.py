
from Funnel import  base_load
from  ServiceWorker import  worker
from  Consumer.consumer import get_results ,  dell_row , add_result
from  No_SQL_DB.migrator import export_postgresql_data_to_json , import_json_to_mongodb
from  No_SQL_DB.consumer import get_results_mongo , get_collection_column_names

from bson import json_util


def  lr1():
    base = base_load.Database(years=[2021])
    sql = """select  physball ,regname , year  from  zno_data 
                              where physball  NOT LIKE 'nan' 
                              ORDER BY regname"""
    base.load_data()

    f = open("result.txt", "w")

    answer = base.output(sql)
    f.truncate(0)
    f.write(answer)

    f.close()
def lr2():
    old_db_info = 'postgresql://user1:123321@localhost:5432/lr1_dat'
    new_db_info = 'postgresql://user1:123321@localhost:5432/new_lr1_dat'

    worker.migrate_and_populate_data(old_db_info, new_db_info)

def lr3():
    result1  = get_results(100, 120)
    with open(r'resul_fetch_data.txt', 'w') as f:
        for tpl in result1:
            line = ' '.join(map(str, tpl))
            f.write(line + '\n')

    result2 = add_result("eecfcec", 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100)

    with open(r'resul_write_data.txt', 'w') as f:
        f.write(result2)

    result3  = dell_row("eecfcec")

    with open(r'resul_del.txt', 'w') as f:
        f.write(result3)

def lr5_loader():
    tables = ['student', 'student_balls', 'student_location']
    export_postgresql_data_to_json(tables )
    json_files = ['student.json', 'student_balls.json', 'student_location.json']
    mongo_database = 'lr_5_data'
    mongo_collections =  ['student', 'student_balls', 'student_location']

    import_json_to_mongodb(json_files, mongo_database, mongo_collections)

def lr5_consumer():

    data_from_mongo_db  =  get_results_mongo('student_location')


    try:
        with open(r'resul_mongo.txt', 'w') as file:

            json_data = json_util.dumps(data_from_mongo_db , indent=2)


            file.write(json_data)

        print(f'Dictionary written to resul_mongo.txt successfully.')
    except Exception as e:
        print(f"Error during file write: {str(e)}")


if __name__ == '__main__':

    lr5_consumer()
