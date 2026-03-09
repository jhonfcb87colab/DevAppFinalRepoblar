import pyodbc
from datetime import datetime
from ttkbootstrap.dialogs import Messagebox
import sys # Importante para un cierre limpio
from ldap3 import Server, Connection, ALL, NTLM
from ldap3 import Server, Connection, ALL, NTLM

class ConexionSQL:
    @staticmethod
    def ejecutar_sp(fecha_inicio, fecha_fin, sp_name, parent_window=None):
        try:


            AD_SERVER = "10.10.3.63"   # o "ldaps://dc.tudominio.local"
            
            #SERVIENT\inteligencianegocios
            DOMAIN = "SERVIENT.COM.CO"
            USER = "inteligencianegocios"
            PASSWORD = "N3g0c10S3rv13ntr3g4TGB.159yhn753.*-"
            
            server = Server(AD_SERVER, get_info=ALL)
            
            conn = Connection(
                server,
                user=f"{DOMAIN}\\{USER}",
                password=PASSWORD,
                authentication=NTLM,
                auto_bind=True
            )
            
            print("Bind OK:", conn.bound)


            # # f_ini = datetime.strptime(fecha_inicio, '%Y-%m-%d').strftime('%Y%m%d') 
            # # f_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').strftime('%Y%m%d') 

            # # conn_str = (
            # #     "DRIVER={ODBC Driver 17 for SQL Server};"
            # #     "SERVER=SERV-DBI01;"
            # #     "DATABASE=SIG_COL_LOGISTICA_DW;"
            # #     "Trusted_Connection=yes;"
            # #     "TrustServerCertificate=yes;"
            # # )

            
#     @Date_Inicio NVarchar(10) = NULL  --  CONVERT( VARCHAR, GETDATE() , 23 )
#    ,@Date_Final  NVarchar(10) = NULL  --  CONVERT( VARCHAR, DATEADD( DAY, -1, GETDATE() ), 23 )
#    ,@mensajeErr NVarchar(200) OUTPUT

            sql_comando = f"""
            SET NOCOUNT ON;
            DECLARE @mensajeErr_out NVARCHAR(200);
            EXEC {sp_name} 
                @Date_Inicio = ?, 
                @Date_Final = ?, 
                @mensajeErr = @mensajeErr_out OUTPUT;
            SELECT @mensajeErr_out;
            """

            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql_comando, (f_ini, f_fin))
                row = cursor.fetchone()
                mensaje_sp = row[0] if row else "Proceso finalizado (sin mensaje)."
                conn.commit()

            # Al dar clic en OK aquí, el código sigue a la siguiente línea
            Messagebox.show_info(
                message=f"📢 Respuesta del servidor:\n\n{mensaje_sp}",
                title="Resultado"
            )

            # --- SCRIPT DE DESTRUCCIÓN ---
            if parent_window:
                parent_window.after(100, parent_window.destroy) 
            else:
                sys.exit() # Cierre de emergencia si no hay ventana vinculada
            
            return mensaje_sp

        except Exception as e:
            error_limpio = str(e).replace("\\n", " ").replace("\\t", " ")
            Messagebox.show_error(
                message=f"❌ Error crítico:\n{error_limpio}",
                title="Error de Ejecución"
            )
            return error_limpio

































# import pyodbc
# from datetime import datetime
# from ttkbootstrap.dialogs import Messagebox
# import sys
# from ldap3 import Server, Connection, ALL, NTLM

# class ConexionSQL:
#     @staticmethod
#     def ejecutar_sp(fecha_inicio, fecha_fin, sp_name, parent_window=None):
#         try:
#             # --- 1. VALIDACIÓN EN EL DOMINIO (LDAP) ---
#             AD_SERVER = "10.10.3.63"
#             DOMAIN = "SERVIENT.COM.CO"
#             USER = "inteligencianegocios"
#             PASSWORD = "N3g0c10S3rv13ntr3g4TGB.159yhn753.*-"
            
#             server = Server(AD_SERVER, get_info=ALL)
#             # Usamos el formato DOMINIO\Usuario para NTLM
#             conn_ldap = Connection(
#                 server,
#                 user=f"SERVIENT\\{USER}", # Ajustado según tu comentario
#                 password=PASSWORD,
#                 authentication=NTLM,
#                 auto_bind=True
#             )
            
#             if not conn_ldap.bound:
#                 raise Exception("No se pudo autenticar en el Dominio (LDAP).")
            
#             print("✅ Autenticación de Dominio Exitosa")

#             # --- 2. CONFIGURACIÓN DE SQL SERVER ---
#             f_ini = datetime.strptime(fecha_inicio, '%Y-%m-%d').strftime('%Y%m%d') 
#             f_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').strftime('%Y%m%d') 

#             # IMPORTANTE: Para usar credenciales de dominio en SQL desde Python,
#             # a veces es necesario enviarlas explícitamente si el proceso no corre como ese usuario.

#             conn_str = (
#                 "DRIVER={ODBC Driver 17 for SQL Server};"
#                 "SERVER=SERV-DBI01;"
#                 "DATABASE=SIG_COL_LOGISTICA_DW;"
#                 "Trusted_Connection=yes;"
#                 "TrustServerCertificate=yes;"
#             )



#             # conn_str = (
#             #     "DRIVER={ODBC Driver 17 for SQL Server};"
#             #     "SERVER=SERV-DBI01;"
#             #     "DATABASE=SIG_COL_LOGISTICA_DW;"
#             #     f"UID={USER}@{DOMAIN};" 
#             #     f"PWD={PASSWORD};"
#             #     "TrustServerCertificate=yes;"
#             # )





#             sql_comando = f"""
#             SET NOCOUNT ON;
#             DECLARE @mensajeErr_out NVARCHAR(200);
#             EXEC {sp_name} 
#                 @Date_Inicio = ?, 
#                 @Date_Final = ?, 
#                 @mensajeErr = @mensajeErr_out OUTPUT;
#             SELECT @mensajeErr_out;
#             """

#             # --- 3. EJECUCIÓN ---
#             with pyodbc.connect(conn_str) as conn_sql:
#                 cursor = conn_sql.cursor()
                
#                 cursor.execute("SELECT SUSER_SNAME(), ORIGINAL_LOGIN();")
#                 print(f"Usuario actual en SQL: {cursor.fetchone()}")                
                
#                 # cursor.execute(sql_comando, (f_ini, f_fin))
#                 # row = cursor.fetchone()
#                 # mensaje_sp = row[0] if row else "Proceso finalizado."
#                 conn_sql.commit()




#             Messagebox.show_info(
#                 message=f"📢 Respuesta del servidor:\n\n{mensaje_sp}",
#                 title="Resultado"
#             )

#             if parent_window:
#                 parent_window.after(100, parent_window.destroy) 
            
#             return mensaje_sp

#         except Exception as e:
#             error_limpio = str(e).replace("\\n", " ").replace("\\t", " ")
#             Messagebox.show_error(
#                 message=f"❌ Error crítico:\n{error_limpio}",
#                 title="Error de Ejecución"
#             )
#             return error_limpio






























































































# class ConexionSQL:
#     @staticmethod
#     def ejecutar_sp(fecha_inicio, fecha_fin, sp_name):
#         try:
#             # 1. Conversión de fechas a formato YYYYMMDD
#             f_ini = datetime.strptime(fecha_inicio, '%Y-%m-%d').strftime('%Y%m%d') 
#             f_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').strftime('%Y%m%d') 

#             conn_str = (
#                 "DRIVER={ODBC Driver 17 for SQL Server};"
#                 "SERVER=D_SERV-DBI01;"
#                 "DATABASE=SIG_COLOMBIA_DW;"
#                 "Trusted_Connection=yes;"
#                 "TrustServerCertificate=yes;"
#             )

#             # 2. SQL limpio sin saltos de línea ni DECLAREs complejos
#             # Usamos SET NOCOUNT ON para evitar el error "Previous SQL was not a query"
#             sql_comando = """
#             SET NOCOUNT ON;
#             DECLARE @mensajeErr_out NVARCHAR(200);
#             EXEC [TMP].[REPOBLAMIENTO_LLAMAR_ETL_EJECUTAR_SP_ENVIOS_MOVILIZADOS_2] 
#                 @FECHA_INICIAL = ?, 
#                 @FECHA_FINAL = ?, 
#                 @mensajeErr = @mensajeErr_out OUTPUT;
#             SELECT @mensajeErr_out;
#             """

#             with pyodbc.connect(conn_str) as conn:
#                 cursor = conn.cursor()
                
#                 # Ejecución parametrizada segura
#                 cursor.execute(sql_comando, (f_ini, f_fin))
                
#                 # 3. Capturar el resultado del SELECT final
#                 row = cursor.fetchone()
#                 mensaje_sp = row[0] if row else "Proceso finalizado (sin mensaje)."
                
#                 conn.commit()

#             Messagebox.show_info(
#                 message=f"📢 Respuesta del servidor:\n\n{mensaje_sp}",
#                 title="Resultado"
#             )

#             return mensaje_sp

#         except Exception as e:
#             # Limpiamos el error para que sea legible en la ventana
#             error_limpio = str(e).replace("\\n", " ").replace("\\t", " ")
#             Messagebox.show_error(
#                 message=f"❌ Error crítico:\n{error_limpio}",
#                 title="Error de Ejecución"
#             )
#             return error_limpio



























    # def ejecutar_sp(fecha_inicio, fecha_fin, sp_name):



    #     try:

    #         fecha_inicial_str =  datetime.strptime(fecha_inicio, '%Y-%m-%d').strftime('%Y%m%d') 
    #         fecha_final_str   =  datetime.strptime(fecha_fin, '%Y-%m-%d').strftime('%Y%m%d') 

    #         conn_str = (
    #             "DRIVER={ODBC Driver 17 for SQL Server};"
    #             "SERVER=D_SERV-DBI01;"
    #             "DATABASE=SIG_COLOMBIA_DW;"  #SIG_COL_LOGISTICA_DW
    #             "Trusted_Connection=yes;"
    #             "TrustServerCertificate=yes;"
    #         )

    #         with pyodbc.connect(conn_str) as conn:
    #             cursor = conn.cursor()

    #             # Declaramos variable OUTPUT y la capturamos


    #             # query = f"""
    #             # DECLARE @mensaje NVARCHAR(200);
    #             # EXEC {sp_name}
    #             #     @Date_Inicio = ?,
    #             #     @Date_Final = ?,
    #             #     @mensajeErr = @mensaje OUTPUT;

    #             # SELECT @mensaje AS mensaje;
    #             # """

    #             scriptSQL_ejecutar = f""""
    #                                         DECLARE	@return_value int,
    #                                                 @mensajeErr nvarchar(200)

    #                                         SELECT	@mensajeErr = N'error'

    #                                         EXEC	@return_value = [TMP].[REPOBLAMIENTO_LLAMAR_ETL_EJECUTAR_SP_ENVIOS_MOVILIZADOS_2]
    #                                                 @FECHA_INICIAL = '{fecha_inicial_str}',
    #                                                 @FECHA_FINAL = N'{fecha_final_str}',
    #                                                 @mensajeErr = @mensajeErr OUTPUT

    #                                         SELECT	@mensajeErr as N'@mensajeErr'  
    #                                 """""    

    #             print(f'Script a ejecutar: {scriptSQL_ejecutar}  ')

    #             ##cursor.execute(query, fecha_inicio, fecha_fin,'error')

    #             cursor.execute(scriptSQL_ejecutar)

    #             # Capturamos el mensaje devuelto
    #             row = cursor.fetchone()
    #             mensaje_sp = row[0] if row else "Sin mensaje devuelto."

    #             conn.commit()

    #         # Mostrar mensaje del SP
    #         Messagebox.show_info(
    #             message=f"📢 Respuesta del SP:\n\n{mensaje_sp}",
    #             title="Resultado del Proceso"
    #         )

    #         return mensaje_sp

    #     except Exception as e:
    #         print('Entra detro del error')
    #         Messagebox.show_error(
    #             message=f"❌ Error ejecutando SP:\n{str(e)}",
    #             title="Error"
    #         )

    #         return str(e)





    # def ejecutar_sp(fecha_inicio, fecha_fin, sp_name):

    #     try:

    #         conn_str = (
    #             "DRIVER={ODBC Driver 17 for SQL Server};"
    #             "SERVER=D_SERV-DBI01;"
    #             "DATABASE=SIG_COLOMBIA_DW;"
    #             "Trusted_Connection=yes;"
    #             "TrustServerCertificate=yes;"
    #         )

    #         with pyodbc.connect(conn_str) as conn:
    #             cursor = conn.cursor()

    #             query = f"EXEC {sp_name} ?, ?"
    #             cursor.execute(query, fecha_inicio, fecha_fin)

    #             conn.commit()
    #         Messagebox.show_info(
    #             message="✅ El Stored Procedure finalizó correctamente.",
    #             title="Proceso Finalizado"
    #         )

    #         return "✅ Stored Procedure ejecutado correctamente."

    #     except Exception as e:
    #         return f"❌ Error ejecutando SP:\n{str(e)}"
