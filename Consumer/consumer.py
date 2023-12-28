import os
import psycopg2


def get_results( start_date, end_date):
    connection = psycopg2.connect("postgresql://user1:123321@localhost:5432/new_lr1_dat")
    cursor = connection.cursor()

    query = """
        SELECT *
        FROM student_balls
        WHERE ukrball100  BETWEEN %s AND %s ;
    """
    cursor.execute(query, ( start_date, end_date))
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return results



def dell_row(outid):
    connection = psycopg2.connect("postgresql://user1:123321@localhost:5432/new_lr1_dat")
    cursor = connection.cursor()

    query = 'DELETE FROM student_balls WHERE outid = %s;'
    cursor.execute(query, (outid,))  # Notice the comma to create a single-value tuple
    connection.commit()

    cursor.close()
    connection.close()

    return "Deleting data successful!"

def add_result(outid, ukrball100, histball100, mathball100, physball100, chemball100, bioball100, geoball100, fraball100, deuball100, spaball100, engball100):

    connection = psycopg2.connect("postgresql://user1:123321@localhost:5432/new_lr1_dat")
    cursor = connection.cursor()

    query = """
        INSERT INTO student_balls (outid, ukrball100, histball100, mathball100, physball100, chemball100, bioball100, geoball100, fraball100, deuball100, spaball100, engball100)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (outid) DO UPDATE
        SET
            ukrball100 = EXCLUDED.ukrball100,
            histball100 = EXCLUDED.histball100,
            mathball100 = EXCLUDED.mathball100,
            physball100 = EXCLUDED.physball100,
            chemball100 = EXCLUDED.chemball100,
            bioball100 = EXCLUDED.bioball100,
            geoball100 = EXCLUDED.geoball100,
            fraball100 = EXCLUDED.fraball100,
            deuball100 = EXCLUDED.deuball100,
            spaball100 = EXCLUDED.spaball100,
            engball100 = EXCLUDED.engball100
    """
    cursor.execute(query, (outid, ukrball100, histball100, mathball100, physball100, chemball100, bioball100, geoball100, fraball100, deuball100, spaball100, engball100))
    connection.commit()

    cursor.close()
    connection.close()

    return "Transaction  successful!"







