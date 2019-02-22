import sys
import mysql.connector

cnx = mysql.connector.connect(user='root',
                              password=sys.argv[1],
                              database='yelp_db')
cursor = cnx.cursor()

statement1 = '''CREATE TABLE prices
            ( id INT NOT NULL AUTO_INCREMENT,
            business_id VARCHAR(22) NOT NULL,
            price VARCHAR(4) NULL,
            PRIMARY KEY (id)) ENGINE = InnoDB'''
cursor.execute(statement1)

statement2 = '''CREATE TABLE business (
             id VARCHAR(22) NOT NULL,
             name VARCHAR(255) NULL,
             neighborhood VARCHAR(255) NULL,
             address VARCHAR(255) NULL,
             city VARCHAR(255) NULL,
             state VARCHAR(255) NULL,
             postal_code VARCHAR(255) NULL,
             latitude FLOAT NULL,
             longitude FLOAT NULL,
             stars FLOAT NULL,
             review_count INT NULL,
             is_open TINYINT(1) NULL,
             PRIMARY KEY (id))
             ENGINE = InnoDB'''
cursor.execute(statement2)

statement3 = '''CREATE TABLE category (
             id INT NOT NULL AUTO_INCREMENT,
             business_id VARCHAR(22) NOT NULL,
             category VARCHAR(255) NULL,
             PRIMARY KEY (id))
             ENGINE = InnoDB'''
cursor.execute(statement3)

cnx.commit()
cnx.close()
