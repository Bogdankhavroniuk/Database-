import psycopg2

from time import perf_counter
import pandas  as  pd
import time


def runtime(func):
    """Deccorator  to get  Runtime  in output  """

    def wrapper(*args, **kwargs):
        print(
            f"---------------------------------------------------{func.__name__}--------------------------------------------------------------------")
        start_time: float = perf_counter()

        function_res = func(*args, *kwargs)

        end_time = perf_counter()

        runtime: float = round((end_time - start_time), 3)

        massage: str = f"{func.__name__} runtime is {runtime} seconds \n "

        f = open("runtime.txt", "w")
        f.truncate(0)
        f.write(massage)

        f.close()

        return function_res

    return wrapper


def conc(dataframes):
    res_df = pd.concat(dataframes, ignore_index=True, sort=False)

    return res_df


class Database():

    def __init__(self, years):

        self.database_name = "lr1_dat"

        self.user_name = "user1"

        self.password = "123321"

        self.host = "localhost"

        self.port = "5432"

        self.years = years

    @runtime
    def load_data(self):

        """"Create and  load data to datbase  """

        # create  conn
        years = self.years

        file_links = [f"Sources/OData{year}File.csv"
                      if year in range(2019, 2023)
                      else f"app/Sources/OpenData{year}.csv"
                      for year in years]

        year_links = {years[i]: file_links[i] for i in range(len(years))}
        i = 0

        # load  data  to  dtaframe  to extract it later into postgres

        dataframes = []
        for year in years:

                encoding =  'ANSI'
                mylist = []
                for chunk in pd.read_csv(year_links[year], delimiter=';', encoding=encoding, low_memory=False,
                                         chunksize=2000):
                    mylist.append(chunk)
                df = pd.concat(mylist, axis=0)
                del mylist

                if year == 2021:
                    df = df.rename(columns={'п»ї"OUTID"': 'OUTID'})
                df.columns = df.columns.str.lower()
                df["year"] = [year] * df.shape[0]

                dataframes.append(df)

        result_df = conc(dataframes)

        del dataframes

        # wrrite values  in  sql table
        conn = psycopg2.connect("dbname=lr1_dat user=user1 password=123321 ")
        cur = conn.cursor()

        drop_table_query = f"DROP TABLE IF EXISTS ZNO_DATA"
        cur.execute(drop_table_query)

        conn.commit()
        table_columns = ','.join(f"{col} TEXT" for col in result_df.columns.values)

        column_names = ','.join(result_df.columns)

        create_table_query = f"CREATE TABLE  ZNO_DATA ({table_columns})" + ";"
        cur.execute(create_table_query)
        conn.commit()
        """
        data = [tuple(row) for row in result_df.to_numpy()]
        execute_values(cur, f"INSERT INTO ZNO_DATA ({column_names}) VALUES %s", data)
        """
        for index, row in df.iterrows():
            values = tuple(str(val).replace('\'', '') for val in result_df.loc[index].values)

            cur.execute(f"INSERT INTO ZNO_DATA ({column_names}) VALUES  {values}")
            conn.commit()

        # Commit the changes to the database
        del df
        conn.close

        del result_df

    def renovated_execute(self, query):
        while True :
           conn = psycopg2.connect("dbname=lr1_dat user=user1 password=123321 ")
           try:
               cur = conn.cursor()
               cur.execute(query)

               date = cur.fetchall()
               conn.close()
               False
               return date
           except:
            time.sleep(2)

            self.renovated_execute(self, conn, query)

    def get_data(self, query):

        """execute query to database"""

        date = self.renovated_execute(query)

        return date

    def output(self , query  ):
        """crete   data to output var question"""

        date = self.get_data(query)

        query_result = pd.DataFrame(columns=["бал", "регіон", "рік"], data=date)
        query_result[['бал', 'рік']] = query_result[['бал', 'рік']].apply(pd.to_numeric)
        answer = query_result.groupby(['регіон', 'рік'])['бал'].mean()

        return answer.to_string()