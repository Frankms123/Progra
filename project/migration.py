from decimal import Decimal
from connectdb import connect_mysql, connect_mongodb, get_tables_mysql


def convertir_decimales_a_float(datos):
    for registro in datos:
        for campo, valor in registro.items():
            if isinstance(valor, Decimal):
                registro[campo] = float(valor)
    return datos


def extraer_datos_mysql(tabla):
    conexion = connect_mysql()
    if not conexion:
        print(f"No se pudo conectar a la tabla {tabla}")
        return []

    try:
        cursor = conexion.cursor()
        cursor.execute(f"SELECT * FROM {tabla};")

        nombres_columnas = [columna[0] for columna in cursor.description]
        datos = [dict(zip(nombres_columnas, fila)) for fila in cursor.fetchall()]
        return datos
    except Exception as e:
        print(f"Error al extraer datos de {tabla}: {e}")
        return []
    finally:
        conexion.close()


def insertar_en_mongodb(base_datos, nombre_tabla, datos):
    datos_convertidos = convertir_decimales_a_float(datos)
    coleccion = base_datos[nombre_tabla.lower()]

    if datos_convertidos:
        coleccion.delete_many({})
        coleccion.insert_many(datos_convertidos)
        print(f"Datos insertados en la colección '{nombre_tabla}' en MongoDB.")
    else:
        print(f"No hay datos para insertar en la colección '{nombre_tabla}'.")


def crear_orden_total(base_datos):
    colecciones = ["orders", "order_details", "products", "customers", "employees"]

    for coleccion in colecciones:
        if base_datos[coleccion].count_documents({}) == 0:
            print(f"La colección {coleccion} está vacía. No se puede crear OrderTotal.")
            return

    base_datos["ordertotal"].delete_many({})
    pipeline_agregacion = [
        {
            "$lookup": {
                "from": "order_details",
                "localField": "OrderID",
                "foreignField": "OrderID",
                "as": "detalles_orden"
            }
        },
        {
            "$lookup": {
                "from": "products",
                "localField": "detalles_orden.ProductID",
                "foreignField": "ProductID",
                "as": "productos"
            }
        },
        {
            "$lookup": {
                "from": "customers",
                "localField": "CustomerID",
                "foreignField": "CustomerID",
                "as": "cliente"
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "EmployeeID",
                "foreignField": "EmployeeID",
                "as": "empleado"
            }
        },
        {
            "$project": {
                "OrderID": 1,
                "OrderDate": 1,
                "ShippedDate": 1,
                "detalles_orden": 1,
                "productos": 1,
                "cliente": {"$arrayElemAt": ["$cliente", 0]},
                "empleado": {"$arrayElemAt": ["$empleado", 0]}
            }
        }
    ]
    resultado = list(base_datos["orders"].aggregate(pipeline_agregacion))

    if resultado:
        base_datos["ordertotal"].insert_many(resultado)
        print(f"Colección OrderTotal creada.")
    else:
        print("No se pudieron agregar datos a OrderTotal.")


def migrar_datos():
    try:
        base_datos = connect_mongodb()
        if base_datos is None:
            print("No se pudo conectar a MongoDB")
            return

        tablas = get_tables_mysql()

        if not tablas:
            print("No se encontraron tablas para migrar")
            return

        for tabla in tablas:
            datos = extraer_datos_mysql(tabla)
            insertar_en_mongodb(base_datos, tabla, datos)
        crear_orden_total(base_datos)
    except Exception as e:
        print(f"Error durante la migración: {e}")

#----------------------------------CONSULTAS-----------------------------------
def consulta_categorias_con_productos(db):
    pipeline = [{
        "$lookup": {
            "from": "products",
            "localField": "CategoryID",
            "foreignField": "CategoryID",
            "as": "productos"
        }
    },
        {
            "$project": {
                "CategoryID": 1,
                "CategoryName": 1,
                "productos.ProductID": 1,
                "productos.ProductName": 1
            }
        }]
    result = list(db["categories"].aggregate(pipeline))
    for categoria in result:
        print(f"ID categoria: {categoria['CategoryID']}, Nombre Categoría: {categoria['CategoryName']}")
        for producto in categoria["productos"]:
            print(f"ID producto: {producto['ProductID']}, Nombre Producto: {producto['ProductName']}")

def consulta_invent_categoria(db, category_id):
    pipeline = [
        {"$match": {"CategoryID": category_id}},
        {
            "$lookup": {
                "from": "products",
                "localField": "CategoryID",
                "foreignField": "CategoryID",
                "as": "productos"
            }
        },
        {
            "$unwind": "$productos"
        },
        {
            "$project": {
                "ID Producto": "$productos.ProductID",
                "Nombre Producto": "$productos.ProductName",
                "Precio Unitario": "$productos.UnitPrice",
                "Existencia": "$productos.UnitsInStock",
                "Total linea": {"$multiply": ["$productos.UnitPrice", "$productos.UnitsInStock"]}
            }
        }
    ]
    result = list(db["categories"].aggregate(pipeline))
    total_categoria = 0
    for producto in result:
        print(f"ID Producto: {producto['ID Producto']}, Nombre: {producto['Nombre Producto']},"
              f"Precio Unitario: {producto['Precio Unitario']}, Existencia: {producto['Existencia']},"
              f"Total Linea: {producto['Total linea']}")
        total_categoria += producto['Total linea']
    print(f"Total Categoria: {total_categoria}")

def consulta_factura(db, order_id):
    pipeline = [
        {"$match": {"OrderID": order_id}},
        {
            "$lookup": {
                "from": "order_details",
                "localField": "OrderID",
                "foreignField": "OrderID",
                "as": "Detalles"
            }
        },
        {
            "$lookup": {
                "from": "customers",
                "localField": "CustomerID",
                "foreignField": "CustomerID",
                "as": "Cliente"
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "EmployeeID",
                "foreignField": "EmployeeID",
                "as": "Trabajador"
            }
        },
        {
            "$project": {
                "OrderID": 1,
                "OrderDate": 1,
                "ShippedDate": 1,
                "Detalles": 1,
                "Cliente": {"$arrayElemAt": ["$Cliente", 0]},
                "Trabajador": {"$arrayElemAt": ["$Trabajador", 0]}
            }
        }
    ]
    factura = db["orders"].aggregate(pipeline)
    for detalle in factura:
        print(f"Factura ID: {detalle['OrderID']}, Fecha Orden: {detalle['OrderDate']}, Fecha Envío: {detalle['ShippedDate']}")
        print(f"Cliente: {detalle['Cliente']['ContactName']}, Trabajador: {detalle['Trabajador']['FirstName']} {detalle['Trabajador']['LastName']}")
        print("Detalles:")
        for item in detalle["Detalles"]:
            print(f"Producto: {item['ProductID']}, Cantidad: {item['Quantity']}, Precio Unitario: {item['UnitPrice']}")

if __name__ == "__main__":
    db_mongo = connect_mongodb()
    migrar_datos()

    #Consulta 1
    print("Lista de categorias con productos:")
    consulta_categorias_con_productos(db_mongo)

    #Consulta 2
    consulta_invent_categoria(db_mongo, 1)

    #Consulta 3
    consulta_factura(db_mongo, 10248)