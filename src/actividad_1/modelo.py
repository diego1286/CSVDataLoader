from sqlalchemy import create_engine, text
import pandas as pd 


class Modelo:

    def __init__(self, host= "", port="", nombredb="", user="", password="",):
        self.host=host
        self.port=port
        self.nombredb=nombredb
        self.user=user
        self.password=password
        self.conexion= None
        self.connexion()

    def connexion(self):
        try:
            self.conexion = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.nombredb}')
            with self.conexion.connect() as conexion:
                print("conexion a la base de datos exitosa")
        except print(0):
            print("error en la conexion a la base de datos")
    
    def create_table(self, ruta_sql=""):
        try:
            with open(ruta_sql, 'r') as file:
                script_tabla= file.read()
            with self.conexion.connect() as conexion:
                conexion= conexion.execution_options(isolation_level="AUTOCOMMIT")
                conexion.execute(text(script_tabla))
                print("creacion de la tabla con exito")
            print(0)
        except print(0):
            print("Error al crear la tabla")
    
    def create_schema(self, nombre_schema):
        try:
            with self.conexion.connect() as conexion:
                create_schema = f'CREATE SCHEMA IF NOT EXISTS {nombre_schema};'
                conexion = conexion.execution_options(isolation_level="AUTOCOMMIT")
                conexion.execute(text(create_schema))
                print("Creación del Schema exitosa")
        except Exception as e:  
            print(f"Error en la creación del Schema: {e}")

    def insert_df(self, ruta_csv="", nombre_schema="", nombre_tabla="", tipo_insert='append', tipo="csv"):
        try:
            
            if tipo == "csv":
                df = pd.read_csv(ruta_csv, index_col=False,  encoding='latin1', sep=',', on_bad_lines='skip')
            else:
                df = pd.read_excel(ruta_csv, index_col=False)
            
            print("Se creó el dataframe")
        except Exception as e:  
            print(f"Error al crear el dataframe: {e}")
        
        schema_tabla = f"{nombre_schema}.{nombre_tabla}"
        try:
            
            df.to_sql(nombre_tabla, con=self.conexion, schema=nombre_schema, if_exists=tipo_insert, index=False)
            print("Se insertó correctamente")
        except Exception as e:  
            print(f"Error al insertar el dataframe: {e}")

    def contar_registros(self, nombre_schema, nombre_tabla):
        try:
            query = f'SELECT COUNT(*) FROM {nombre_schema}.{nombre_tabla};'
            with self.conexion.connect() as conexion:
                result = conexion.execute(text(query))
                count = result.scalar()  
            return count
        except Exception as e:
            print(f"Error al contar los registros: {e}")
            return None

    
    