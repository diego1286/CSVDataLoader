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

    def prueba_integridad(self, nombre_schema, nombre_tabla):
        try:
        # 1. Verificar si existen valores nulos en las columnas no nulas
            query_nulls = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{nombre_schema}'
                AND table_name = '{nombre_tabla}'
                AND is_nullable = 'NO'
                """
            with self.conexion.connect() as conexion:
                result_nulls = conexion.execute(text(query_nulls))
                non_nullable_columns = [row['column_name'] for row in result_nulls]
        
        # analizar si hay registros con valores nulos en las columnas no nulas
            if non_nullable_columns:
                null_check_query = f"""
                    SELECT {', '.join(non_nullable_columns)}
                    FROM {nombre_schema}.{nombre_tabla}
                    WHERE {" OR ".join([f"{col} IS NULL" for col in non_nullable_columns])}
                """
                with self.conexion.connect() as conexion:
                    result = conexion.execute(text(null_check_query))
                    null_rows = result.fetchall()
                    if null_rows:
                        print(f"Se encontraron registros con valores nulos en las columnas no nulas: {non_nullable_columns}")
                        return False
            else:
                print("No se encontraron columnas no nulas, omitiendo la verificación de valores nulos.")
            
        # 2. Comprobación de unicidad de la clave primaria 
            query_primary_key = f"""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{nombre_schema}.{nombre_tabla}'::regclass
                AND i.indisprimary
            """
            with self.conexion.connect() as conexion:
                result_primary_key = conexion.execute(text(query_primary_key))
                primary_key_columns = [row['attname'] for row in result_primary_key]
            
            # Verificar si la clave primaria tiene duplicados
            if primary_key_columns:
                pk_check_query = f"""
                    SELECT {', '.join(primary_key_columns)}
                    FROM {nombre_schema}.{nombre_tabla}
                    GROUP BY {', '.join(primary_key_columns)}
                    HAVING COUNT(*) > 1
                """
                with self.conexion.connect() as conexion:
                    result = conexion.execute(text(pk_check_query))
                    duplicates = result.fetchall()
                    if duplicates:
                        print(f"Se encontraron registros duplicados en la clave primaria: {primary_key_columns}")
                        return False

            print("La prueba de integridad fue exitosa. No se encontraron problemas.")
            return True
        except Exception as e:
            print(f"Error al realizar la prueba de integridad: {e}")
            return False
