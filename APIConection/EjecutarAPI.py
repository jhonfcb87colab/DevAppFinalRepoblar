import http.client
import json
import base64
from datetime import datetime
import sys 

class EjecutarAPI:
    @staticmethod
    def llamar_servicio(servidor, base_datos, sp_ejecutar, fech_ini, fech_fin):
        # --- VOLVEMOS A HTTP (Sin la S) ---
        # El error WRONG_VERSION_NUMBER confirma que el puerto 5555 es HTTP
        conn = http.client.HTTPConnection("t_serv-dbi01", 5555)

        # Limpieza de variables (quitando posibles espacios invisibles)
        SERVIDOR = servidor #'D_SERV-DBI01'
        BASE_DE_DATOS = base_datos #'SIG_COLOMBIA_DW'
        SP_EJECUTAR = sp_ejecutar #'[TMP].[REPOBLAMIENTO_LLAMAR_ETL_EJECUTAR_SP_ENVIOS_MOVILIZADOS_2]'
        FECH_INI = fech_ini #'2026-01-01'
        FECH_FIN = fech_fin #'2026-02-01'

        # Generación dinámica del Token
        time_now = datetime.now()
        str_fecha = time_now.strftime("%Y%m%d") 
        var_connection = f"{SERVIDOR};Database={BASE_DE_DATOS};Integrated Security=true;"
        str_para_tekenizar = f"{str_fecha}|{var_connection}"

        token_dinamico = base64.b64encode(str_para_tekenizar.encode("utf-8")).decode("utf-8")

        payload = json.dumps({
            "Param1": FECH_INI,
            "Param2": FECH_FIN,
            "Param3": SP_EJECUTAR,
            "Param4": token_dinamico
        })

        try:
            # Realizar la petición
            conn.request("POST", "/api/datos", payload)#, headersList
            response = conn.getresponse()
            
            print(f"Status: {response.status}")
            print(f"Reason: {response.reason}")
            
            result = response.read()
            print("\nRespuesta del servidor:")
            print(result.decode("utf-8"))

        except Exception as e:
            print(f"Error al conectar: {e}")

        finally:
            conn.close()
            print("Cerrando programa.")
            sys.exit() # Finaliza el script
        return result.decode("utf-8"),response.status
    
