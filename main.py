# This is a sample Python script.
from Funnel import  base_load
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


if __name__ == '__main__':


    if __name__ == '__main__':
        base = base_load.Database(years=[2021])
        sql = """select  physball ,regname , year  from  zno_data 
                          where physball  NOT LIKE 'nan' 
                          ORDER BY regname"""
        base.load_data()
        answer = base.output(sql)

        f = open("result.txt", "w")
        f.truncate(0)
        f.write(answer)

        f.close()
