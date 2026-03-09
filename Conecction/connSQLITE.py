import sqlite3
import pandas as pd
from datetime import datetime

class ConexionSQLite:
    def __init__(self):
        self.hora_inicio = datetime.now()
        # Ruta de la base de datos
        self.path_db = r"\\T_serv-dbi01\t\App_Automatizacion\Repoblmiento_DB\Credenciales.db"
        self.conn = None
        self._conectar()

    def _conectar(self):
        """Método interno para establecer la conexión."""
        try:
            self.conn = sqlite3.connect(self.path_db)
            print(f"[{datetime.now()}] Conexión a SQLite exitosa.")
        except sqlite3.Error as e:
            print(f"Error crítico al conectar a la base de datos: {e}")
            self.conn = None

    def ejecutar_ddl(self, query_ddl):
        """
        Ejecuta sentencias DDL (CREATE, ALTER, DROP, etc.)
        """
        if not self.conn:
            return "Error: No hay conexión activa."
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_ddl)
            self.conn.commit()
            return f"DDL ejecutado correctamente."
        except sqlite3.Error as e:
            if self.conn: self.conn.rollback()
            return f"Error en DDL: {e}"

    def ejecutar_dql(self, query_dql):
        """
        DQL (Data Query Language): Ejecuta SELECT y devuelve un DataFrame de Pandas.
        """
        if not self.conn:
            print("Error: Conexión no disponible.")
            return None
        
        try:
            # Usamos pandas directamente con la conexión de la clase
            df = pd.read_sql_query(query_dql, self.conn)
            return df
        except Exception as e:
            print(f"Error al recuperar datos: {e}")
            return None

    def ejecutar_dml(self, query_dml):
        """
        DML (Data Manipulation Language): INSERT, UPDATE, DELETE.
        """
        if not self.conn:
            return "Error: Conexión no disponible."
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_dml)
            self.conn.commit()
            return "DML ejecutado correctamente."
        except sqlite3.OperationalError as e:
            if self.conn: self.conn.rollback()
            return f"Error operacional (posible columna duplicada o tabla bloqueada): {e}"
        except sqlite3.Error as e:
            if self.conn: self.conn.rollback()
            return f"Error de SQLite: {e}"

    def cerrar_conexion(self):
        """Cierra la conexión de forma segura."""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("Conexión a base de datos cerrada correctamente.")

    def __del__(self):
        """Asegura el cierre al destruir el objeto."""
        self.cerrar_conexion()

# ────────────────────────────────────────────────
# Ejemplo de uso estructurado
# ────────────────────────────────────────────────
# if __name__ == "__main__":
#     # 1. Instanciar la clase
#     db = ConexionSQLite()

#     if db.conn:
#         # 2. Ejemplo DDL: Crear Tabla
#         tabla_sql = """
#         CREATE TABLE IF NOT EXISTS indicadores_servientrega (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             dashboard TEXT NOT NULL,
#             kpi_valor REAL,
#             fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
#         )
#         """
#         print(db.ejecutar_ddl(tabla_sql))

#         # 3. Ejemplo DML: Insertar datos
#         insert_sql = "INSERT INTO indicadores_servientrega (dashboard, kpi_valor) VALUES ('Eficiencia Recoleccion', 95.5)"
#         print(db.ejecutar_dml(insert_sql))

#         # 4. Ejemplo DQL: Consultar con Pandas
#         query = "SELECT * FROM indicadores_servientrega"
#         df_resultados = db.ejecutar_dql(query)
        
#         if df_resultados is not None:
#             print("\n--- Datos Recuperados ---")
#             print(df_resultados.head())

#     # 5. El cierre es automático por el destructor, pero se puede forzar:
#     db.cerrar_conexion()
