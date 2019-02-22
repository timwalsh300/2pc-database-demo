import sys
import mysql.connector

cnx = mysql.connector.connect(user='root',
                              password=sys.argv[1],
                              database='yelp_db')
cursor = cnx.cursor()

statement = '''DELETE FROM business WHERE
             city = "Las Vegas" AND latitude < 35'''
cursor.execute(statement)
statement2 = '''DELETE FROM business WHERE
             city = "Las Vegas" AND latitude > 37'''
cursor.execute(statement)
statement = '''DELETE FROM business WHERE
             city = "Las Vegas" AND longitude < -116'''
cursor.execute(statement)
statement = '''DELETE FROM business WHERE
             city = "Las Vegas" AND longitude > -114'''
cursor.execute(statement)
statement = '''DROP TABLE hours'''
cursor.execute(statement)
statement = '''DROP TABLE attribute'''
cursor.execute(statement)
statement = '''DROP TABLE user'''
cursor.execute(statement)
statement = '''DROP TABLE review'''
cursor.execute(statement)
statement = '''DROP TABLE friend'''
cursor.execute(statement)
statement = '''DROP TABLE elite_years'''
cursor.execute(statement)
statement = '''DROP TABLE checkin'''
cursor.execute(statement)
statement = '''DROP TABLE tip'''
cursor.execute(statement)
statement = '''DROP TABLE photo'''
cursor.execute(statement)

cnx.commit()
cnx.close()
