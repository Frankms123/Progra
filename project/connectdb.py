# connectdb.py

import pymysql
import pymongo
from config import MYSQL_CONFIG, MONGO_CONFIG

def connect_mysql():
    try:
        conexion_mysql = pymysql.connect(
            host=MYSQL_CONFIG["host"],
            db=MYSQL_CONFIG["db"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"]
        )
        print("Conexión a MySQL exitosa.")
        return conexion_mysql
    except Exception as e:
        print(f"Error de conexión a MySQL: {e}")

def connect_mongodb():
    try:
        client_mongo = pymongo.MongoClient(
            host=MONGO_CONFIG["host"],
            port=MONGO_CONFIG["port"]
        )
        db_mongo = client_mongo[MONGO_CONFIG["db_name"]]
        print("Conexión a MongoDB exitosa.")
        return db_mongo
    except Exception as e:
        print(f"Error de conexión a MongoDB: {e}")

def get_tables_mysql():
    conexion_mysql = connect_mysql()
    if conexion_mysql:
        try:
            cursor = conexion_mysql.cursor()
            cursor.execute("SHOW TABLES;")
            tablas = [tabla[0] for tabla in cursor.fetchall()]
            conexion_mysql.close()
            return tablas
        except Exception as e:
            print(f"Error obteniendo las tablas desde Mysql")
            conexion_mysql.close()
    return []
