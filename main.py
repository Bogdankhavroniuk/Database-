
from Funnel import  base_load
from  ServiceWorker import  worker

if __name__ == '__main__':


    if __name__ == '__main__':
        base = base_load.Database(years=[2021])
        sql = """select  physball ,regname , year  from  zno_data 
                          where physball  NOT LIKE 'nan' 
                          ORDER BY regname"""
        #base.load_data()


        f = open("result.txt", "w")


        answer = base.output(sql)
        f.truncate(0)
        f.write(answer)

        f.close()

        old_db_info = 'postgresql://user1:123321@localhost:5432/lr1_dat'
        new_db_info = 'postgresql://user1:123321@localhost:5432/new_lr1_dat'

        worker.migrate_and_populate_data(old_db_info, new_db_info)