from modelo import Modelo
import pkg_resources


ruta = pkg_resources.resource_filename(__name__, 'static')
print(ruta, "esta es la ruta que se envia ")
host="localhost"
port="5432"
nombredb="cars_electrics"
user="postgres"
password="1286"
nombre_schema="cars_electric"
nombre_tabla="cars_electrics"
ruta_sql = "{}/sql/bd.sql".format(ruta)
ruta_csv = "{}/xlsx/Vehicle.csv".format(ruta)
modelo = Modelo(host, port, nombredb, user, password)
modelo.create_schema(nombre_schema)
modelo.create_table(ruta_sql) 
modelo.insert_df(ruta_csv=ruta_csv, nombre_schema=nombre_schema, nombre_tabla="cars_electrics", tipo_insert='append')
