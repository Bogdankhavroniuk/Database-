import os
import psycopg2
from flask import Flask, render_template, request

def get_results( start_date, end_date):
    connection = psycopg2.connect("postgresql://user1:123321@localhost:5432/new_lr1_dat")
    cursor = connection.cursor()

    query = """
        SELECT *
        FROM student_balls
        WHERE ukrball100  BETWEEN %s AND %s
    """
    cursor.execute(query, ( start_date, end_date))
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return results

def add_result(outid, ukrball100, histball100, mathball100, physball100, chemball100, bioball100, geoball100, fraball100, deuball100, spaball100, engball100):
    connection = psycopg2.connect("postgresql://user1:123321@localhost:5432/new_lr1_dat")
    cursor = connection.cursor()

    query = """
        INSERT INTO student_balls (outid, ukrball100, histball100, mathball100, physball100, chemball100, bioball100, geoball100, fraball100, deuball100, spaball100, engball100)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (outid, ukrball100, histball100, mathball100, physball100, chemball100, bioball100, geoball100, fraball100, deuball100, spaball100, engball100))
    connection.commit()

    cursor.close()
    connection.close()

    print("Дані успішно додано до БД!")


def dell_row(outid):
    connection = psycopg2.connect("postgresql://user1:123321@localhost:5432/new_lr1_dat")
    cursor = connection.cursor()

    query = """
            DELETE FROM student_balls 
            WHERE outid =%s
        """
    cursor.execute(query, (outid))
    connection.commit()

    cursor.close()
    connection.close()

    print("Дані видалені з  БД!")

get_results( 100, 120)
add_result("eecfcec", 100,100, 100, 100, 100, 100, 100, 100, 100, 100, 100)
dell_row("")


