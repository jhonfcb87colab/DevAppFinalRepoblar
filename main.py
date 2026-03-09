import sqlite3
import pandas as pd
import pyodbc
import warnings
import multiprocessing
import ttkbootstrap as tb
import ttkbootstrap as tb
from tksheet import Sheet
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Messagebox
from concurrent.futures import ProcessPoolExecutor
from Conecction.ConnSQL import ConexionSQL
from APIConection.EjecutarAPI import EjecutarAPI
from Conecction import connSQLITE 

warnings.filterwarnings("ignore")

#PATH_DB_SQLITE = "Dataset/Credenciales.db"
PATH_DB_SQLITE = r"\\T_serv-dbi01\t\App_Automatizacion\Repoblmiento_DB\Credenciales.db"
# 🔥 Pool global de procesos
executor = ProcessPoolExecutor(max_workers=1)



class VentanaFechas(tb.Toplevel):

    def __init__(self, parent, titulo_cubo, sp_destino):
        super().__init__(parent)

        self.title(f"Repoblar: {titulo_cubo}")
        self.geometry("500x600")
        #self.wm_state('zoomed')
        self.resizable(False, False)

        self.sp_destino = sp_destino
        self.sevidor = 'SERV-DBI01'
        self.base_datos = 'SIG_COL_LOGISTICA_DW'
        self.grab_set()

        tb.Label(
            self,
            text=f"Configuración para:\n{titulo_cubo}",
            font=("Segoe UI", 12, "bold"),
            justify=CENTER
        ).pack(pady=20)

        frame = tb.Frame(self)
        frame.pack(pady=10)

        tb.Label(frame, text="Fecha Inicial:").grid(row=0, column=0, padx=10, pady=5)
        self.fi = tb.widgets.DateEntry(frame, width=15, dateformat="%Y-%m-%d")
        self.fi.grid(row=0, column=1)

        tb.Label(frame, text="Fecha Final:").grid(row=1, column=0, padx=10, pady=5)
        self.ff = tb.widgets.DateEntry(frame, width=15, dateformat="%Y-%m-%d")
        self.ff.grid(row=1, column=1)

        self.btn = tb.Button(
            self,
            text="🚀 Iniciar Repoblamiento",
            bootstyle="success",
            command=self.ejecutar
        )
        self.btn.pack(pady=20)

        self.progress = tb.Progressbar(
            self,
            mode="indeterminate",
            bootstyle="info-striped",
            length=250
        )

    # ===============================

    def ejecutar(self):

        fi = self.fi.entry.get()
        ff = self.ff.entry.get()

        self.btn.config(state="disabled", text="Procesando...")
        self.progress.pack(pady=15)
        self.progress.start(10)

        self.config(cursor="watch")


        self.future = executor.submit(
            EjecutarAPI.llamar_servicio,
            self.sevidor,
            self.base_datos,
            self.sp_destino,
            fi,
            ff            
        )
        self.after(400, self.verificar_estado)
        

        print("Proceso iniciado en segundo plano...",self.future)


    def verificar_estado(self):
            # Si el proceso ya terminó (status: 200 que ya viste en consola)
            if self.future.done():
                try:
                    resultado = self.future.result()
                    self.progress.stop()
                    
                    Messagebox.show_info(message=f"Respuesta: {resultado}", title="Éxito")
                    
                except Exception as e:
                    Messagebox.show_error(message=f"Error: {e}")
                
                finally:
                    # ESTO ES LO QUE CIERRA LA VENTANA
                    if self.master:
                        self.master.deiconify() # Muestra la ventana principal de nuevo
                    self.destroy() # Cierra la ventana actual de carga
                return

            # Si aún no termina, vuelve a llamar a esta función en 400ms
            self.after(400, self.verificar_estado)


# =====================================
# PANEL PRINCIPAL
# =====================================

class AppPrincipal(tb.Toplevel):

    def __init__(self, parent, rol):
        super().__init__(parent)

        self.title("SISTEMA DE REPOBLAMIENTO BI")
        self.geometry("500x600")

        tb.Label(
            self,
            text=f"PANEL DE CONTROL - {rol}",
            font=("Segoe UI", 18, "bold")
        ).pack(pady=30)

        db = connSQLITE.ConexionSQLite()
        if db.conn:
            opciones = pd.DataFrame()
            if db.conn:
                query = "SELECT * FROM CONFIG_CUBOS"
                opciones = db.ejecutar_dql(query)  
                # Iteramos usando itertuples por eficiencia y claridad
                for fila in opciones.itertuples(index=False):
                    texto_btn = fila.NOMBRE_MOSTRAR
                    valor_sp = fila.SP_ASOCIADO
                    print(texto_btn,' - ',valor_sp)
            
                    # Creamos el botón
                    btn = tb.Button(
                        self,
                        text=texto_btn,
                        bootstyle="primary",
                        width=40
                    )
                    # El comando destruye 'self' (la ventana AppPrincipal) y abre VentanaFechas
                    # Usamos la ventana principal (el root) como padre para que la nueva ventana no muera
                    btn.config(command=lambda t=texto_btn, s=valor_sp: [VentanaFechas(self.master, t, s), self.destroy()])           
                    btn.pack(pady=10)

        tb.Button(
                    self,
                    text="🛑 FINALIZAR TODO EL PROGRAMA",
                    bootstyle="danger", # Rojo sólido para resaltar
                    width=40,
                    command=self.salir_total
                ).pack(pady=20)
        
        # BOTON PANEL ADMIN
        if rol.upper() == "ADMINISTRADOR":

            tb.Button(
                self,
                text="⚙️ PANEL ADMINISTRADOR",
                bootstyle="warning",
                width=40,
                command=lambda: VentanaAdmin(self)
            ).pack(pady=10)
        
    def salir_total(self):
            """Cierra todas las ventanas y finaliza el proceso de Python"""
            self.master.destroy() # Destruye la ventana principal (Root)
            self.quit()           # Sale del mainloop


# =====================================
# LOGIN (VENTANA RAÍZ)
# =====================================

class VentanaLogin(tb.Window):

    def __init__(self):
        super().__init__(themename="darkly")

        self.title("Acceso BI")
        self.geometry("500x600")

        container = tb.Frame(self, padding=40)
        container.pack(expand=True)

        tb.Label(
            container,
            text="LOGIN SISTEMA BI",
            font=("Segoe UI", 18, "bold")
        ).pack(pady=20)

        self.user = tb.Entry(container, width=30)
        self.user.pack(pady=10)
        self.user.insert(0, "admin")

        self.password = tb.Entry(container, width=30, show="*")
        self.password.pack(pady=10)
        self.password.insert(0, "xxxxxxxxxxxxxxx") ##admin123

        tb.Button(
            container,
            text="Ingresar",
            bootstyle="success",
            width=25,
            command=self.login
        ).pack(pady=20)

    # ===============================

    def login(self):
        u = self.user.get()
        p = self.password.get()
        try:
            with sqlite3.connect(PATH_DB_SQLITE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT TIPO_PERMISO_ACCES, ESTADO FROM CREDENCIALES WHERE USUARIO=? AND PASSWORD=?",
                    (u, p)
                )
                datos = cursor.fetchone()
            if datos:
                rol, estado = datos
                if estado == 1:
                    # ocultamos login
                    self.withdraw()
                    AppPrincipal(self, rol)
                else:
                    Messagebox.show_error("Usuario deshabilitado.")
            else:
                Messagebox.show_error("Credenciales incorrectas.")
        except Exception as e:
            Messagebox.show_error(f"Error DB:\n{e}")




class VentanaAdmin(tb.Toplevel):

    def __init__(self, parent):

        super().__init__(parent)

        self.title("⚙ PANEL ADMINISTRADOR")
        self.geometry("1250x350")

        self.db_path = PATH_DB_SQLITE

        # ESTILO TIPO EXCEL
        style = tb.Style()

        style.configure(
            "Treeview",
            background="white",
            foreground="black",
            rowheight=32,
            fieldbackground="white",
            font=("Segoe UI", 12)
        )

        style.configure(
            "Treeview.Heading",
            font=("Segoe UI", 13, "bold")
        )

        style.map(
            "Treeview",
            background=[("selected", "#3399FF")]
        )

        notebook = tb.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_sp = tb.Frame(notebook)
        self.tab_users = tb.Frame(notebook)

        notebook.add(self.tab_sp, text="Administrar SPs")
        notebook.add(self.tab_users, text="Administrar Usuarios")

        self.crear_tab_sp()
        self.crear_tab_users()

    # =====================================
    # TAB SPs
    # =====================================

    def crear_tab_sp(self):

        frame_top = tb.Frame(self.tab_sp)
        frame_top.pack(fill="x", pady=10)

        self.sp_nombre = tb.Entry(frame_top, width=30)
        self.sp_nombre.insert(0, "Nombre a mostrar")
        self.sp_nombre.pack(side="left", padx=5)

        self.sp_asociado = tb.Entry(frame_top, width=45)
        self.sp_asociado.insert(0, "SP_ASOCIADO")
        self.sp_asociado.pack(side="left", padx=5)

        tb.Button(
            frame_top,
            text="Agregar",
            bootstyle="success",
            command=self.agregar_sp
        ).pack(side="left", padx=5)

        tb.Button(
            frame_top,
            text="Eliminar",
            bootstyle="danger",
            command=self.eliminar_sp
        ).pack(side="left", padx=5)

        tb.Button(
            frame_top,
            text="Recargar",
            bootstyle="info",
            command=self.cargar_sps
        ).pack(side="left", padx=5)

        columnas = ("NOMBRE", "SP", "ESTADO")

        self.tree_sp = tb.Treeview(
            self.tab_sp,
            columns=columnas,
            show="headings",
            height=20
        )

        self.tree_sp.heading("NOMBRE", text="NOMBRE MOSTRAR")
        self.tree_sp.heading("SP", text="SP_ASOCIADO")
        self.tree_sp.heading("ESTADO", text="ESTADO")

        self.tree_sp.column("NOMBRE", width=320)
        self.tree_sp.column("SP", width=550)
        self.tree_sp.column("ESTADO", width=100, anchor="center")

        self.tree_sp.pack(fill="both", expand=True)

        self.tree_sp.bind("<Double-1>", self.editar_celda_sp)

        self.cargar_sps()

    # =====================================
    # CARGAR SPS
    # =====================================

    def cargar_sps(self):

        for row in self.tree_sp.get_children():
            self.tree_sp.delete(row)

        with sqlite3.connect(self.db_path) as conn:

            df = pd.read_sql(
                "SELECT NOMBRE_MOSTRAR,SP_ASOCIADO,ESTADO FROM CONFIG_CUBOS",
                conn
            )

        for fila in df.itertuples():
            self.tree_sp.insert(
                "",
                "end",
                values=(fila.NOMBRE_MOSTRAR, fila.SP_ASOCIADO, fila.ESTADO)
            )

    # =====================================
    # AGREGAR SP
    # =====================================

    def agregar_sp(self):

        nombre = self.sp_nombre.get()
        sp = self.sp_asociado.get()

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO CONFIG_CUBOS
                (NOMBRE_MOSTRAR, SP_ASOCIADO, ESTADO)
                VALUES (?, ?, 1)
            """, (nombre, sp))

            conn.commit()

        self.cargar_sps()

    # =====================================
    # ELIMINAR SP
    # =====================================

    def eliminar_sp(self):

        item = self.tree_sp.selection()

        if not item:
            return

        nombre = self.tree_sp.item(item)["values"][0]

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute(
                "DELETE FROM CONFIG_CUBOS WHERE NOMBRE_MOSTRAR=?",
                (nombre,)
            )

            conn.commit()

        self.cargar_sps()

    # =====================================
    # EDITAR CELDA SP
    # =====================================

    def editar_celda_sp(self, event):

        region = self.tree_sp.identify("region", event.x, event.y)

        if region != "cell":
            return

        row_id = self.tree_sp.identify_row(event.y)
        column = self.tree_sp.identify_column(event.x)

        x, y, width, height = self.tree_sp.bbox(row_id, column)

        valor_actual = self.tree_sp.set(row_id, column)

        entry = tb.Entry(self.tree_sp, font=("Segoe UI", 12))
        entry.place(x=x, y=y, width=width, height=height)

        entry.insert(0, valor_actual)
        entry.focus()

        def guardar(event):

            nuevo_valor = entry.get()

            self.tree_sp.set(row_id, column, nuevo_valor)

            valores = self.tree_sp.item(row_id)["values"]

            with sqlite3.connect(self.db_path) as conn:

                cursor = conn.cursor()

                cursor.execute("""
                    UPDATE CONFIG_CUBOS
                    SET NOMBRE_MOSTRAR=?,
                        SP_ASOCIADO=?,
                        ESTADO=?
                    WHERE NOMBRE_MOSTRAR=?
                """,
                (valores[0], valores[1], valores[2], valor_actual))

                conn.commit()

            entry.destroy()

        entry.bind("<Return>", guardar)
        entry.bind("<FocusOut>", lambda e: entry.destroy())

    # =====================================
    # TAB USUARIOS
    # =====================================

    def crear_tab_users(self):

        frame_top = tb.Frame(self.tab_users)
        frame_top.pack(fill="x", pady=10)

        self.new_user = tb.Entry(frame_top, width=25)
        self.new_user.insert(0, "usuario")
        self.new_user.pack(side="left", padx=5)

        self.new_pass = tb.Entry(frame_top, width=25)
        self.new_pass.insert(0, "password")
        self.new_pass.pack(side="left", padx=5)

        tb.Button(
            frame_top,
            text="Crear Usuario",
            bootstyle="success",
            command=self.crear_usuario
        ).pack(side="left", padx=5)

        tb.Button(
            frame_top,
            text="Habilitar / Deshabilitar",
            bootstyle="warning",
            command=self.toggle_usuario
        ).pack(side="left", padx=5)

        tb.Button(
            frame_top,
            text="Recargar",
            bootstyle="info",
            command=self.cargar_users
        ).pack(side="left", padx=5)

        columnas = ("USUARIO", "ROL", "ESTADO")

        self.tree_users = tb.Treeview(
            self.tab_users,
            columns=columnas,
            show="headings",
            height=20
        )

        self.tree_users.heading("USUARIO", text="USUARIO")
        self.tree_users.heading("ROL", text="ROL")
        self.tree_users.heading("ESTADO", text="ESTADO")

        self.tree_users.column("USUARIO", width=280)
        self.tree_users.column("ROL", width=250)
        self.tree_users.column("ESTADO", width=120, anchor="center")

        self.tree_users.pack(fill="both", expand=True)

        self.tree_users.bind("<Double-1>", self.editar_celda_user)

        self.cargar_users()

    # =====================================
    # CARGAR USUARIOS
    # =====================================

    def cargar_users(self):

        for row in self.tree_users.get_children():
            self.tree_users.delete(row)

        with sqlite3.connect(self.db_path) as conn:

            df = pd.read_sql(
                "SELECT USUARIO,TIPO_PERMISO_ACCES,ESTADO FROM CREDENCIALES",
                conn
            )

        for fila in df.itertuples():

            estado = "Activo" if fila.ESTADO == 1 else "Deshabilitado"

            self.tree_users.insert(
                "",
                "end",
                values=(fila.USUARIO, fila.TIPO_PERMISO_ACCES, estado)
            )

    # =====================================
    # CREAR USUARIO
    # =====================================

    def crear_usuario(self):

        u = self.new_user.get()
        p = self.new_pass.get()

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO CREDENCIALES
                (TIPO_PERMISO_ACCES,ID_ROLE,LINK,USUARIO,PASSWORD,TIPO,ESTADO)
                VALUES
                ('OPERADOR',2,'NA',?,?, 'USER',1)
            """, (u, p))

            conn.commit()

        self.cargar_users()

    # =====================================
    # ACTIVAR / DESACTIVAR USUARIO
    # =====================================

    def toggle_usuario(self):

        item = self.tree_users.selection()

        if not item:
            return

        usuario = self.tree_users.item(item)["values"][0]

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute("""
                UPDATE CREDENCIALES
                SET ESTADO = CASE WHEN ESTADO=1 THEN 0 ELSE 1 END
                WHERE USUARIO=?
            """, (usuario,))

            conn.commit()

        self.cargar_users()

    # =====================================
    # EDITAR CELDA USUARIO
    # =====================================

    def editar_celda_user(self, event):

        region = self.tree_users.identify("region", event.x, event.y)

        if region != "cell":
            return

        row_id = self.tree_users.identify_row(event.y)
        column = self.tree_users.identify_column(event.x)

        x, y, width, height = self.tree_users.bbox(row_id, column)

        valor_actual = self.tree_users.set(row_id, column)

        entry = tb.Entry(self.tree_users, font=("Segoe UI", 12))
        entry.place(x=x, y=y, width=width, height=height)

        entry.insert(0, valor_actual)
        entry.focus()

        def guardar(event):

            nuevo_valor = entry.get()

            self.tree_users.set(row_id, column, nuevo_valor)

            valores = self.tree_users.item(row_id)["values"]

            with sqlite3.connect(self.db_path) as conn:

                cursor = conn.cursor()

                cursor.execute("""
                    UPDATE CREDENCIALES
                    SET TIPO_PERMISO_ACCES=?,
                        ESTADO=?
                    WHERE USUARIO=?
                """,
                (valores[1], 1 if valores[2] == "Activo" else 0, valores[0]))

                conn.commit()

            entry.destroy()

        entry.bind("<Return>", guardar)
        entry.bind("<FocusOut>", lambda e: entry.destroy())



class VentanaAdmin(tb.Toplevel):

    def __init__(self, parent):

        super().__init__(parent)

        self.title("⚙ PANEL ADMINISTRADOR")
        self.geometry("950x650")

        self.db_path = PATH_DB_SQLITE

        # ===== ESTILO TIPO EXCEL =====
        style = tb.Style()

        style.configure(
            "Treeview",
            background="white",
            foreground="black",
            rowheight=34,
            font=("Segoe UI", 12)
        )

        style.configure(
            "Treeview.Heading",
            font=("Segoe UI", 13, "bold")
        )

        style.layout(
            "Treeview",
            [('Treeview.treearea', {'sticky': 'nswe'})]
        )

        style.map(
            "Treeview",
            background=[("selected", "#3399FF")]
        )

        notebook = tb.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_sp = tb.Frame(notebook)
        self.tab_users = tb.Frame(notebook)

        notebook.add(self.tab_sp, text="Administrar SPs")
        notebook.add(self.tab_users, text="Administrar Usuarios")

        self.crear_tab_sp()
        self.crear_tab_users()

    # =====================================
    # TAB SPs
    # =====================================

    def crear_tab_sp(self):

        frame_top = tb.Frame(self.tab_sp)
        frame_top.pack(fill="x", pady=10)

        self.sp_nombre = tb.Entry(frame_top, width=30)
        self.sp_nombre.insert(0, "Nombre a mostrar")
        self.sp_nombre.pack(side="left", padx=5)

        self.sp_asociado = tb.Entry(frame_top, width=45)
        self.sp_asociado.insert(0, "SP_ASOCIADO")
        self.sp_asociado.pack(side="left", padx=5)

        tb.Button(frame_top,text="Agregar",bootstyle="success",
                  command=self.agregar_sp).pack(side="left", padx=5)

        tb.Button(frame_top,text="Eliminar",bootstyle="danger",
                  command=self.eliminar_sp).pack(side="left", padx=5)

        tb.Button(frame_top,text="Recargar",bootstyle="info",
                  command=self.cargar_sps).pack(side="left", padx=5)

        columnas = ("NOMBRE", "SP", "ESTADO")

        self.tree_sp = tb.Treeview(
            self.tab_sp,
            columns=columnas,
            show="headings",
            height=20
        )

        self.tree_sp.heading("NOMBRE", text="NOMBRE MOSTRAR")
        self.tree_sp.heading("SP", text="SP_ASOCIADO")
        self.tree_sp.heading("ESTADO", text="ESTADO")

        self.tree_sp.column("NOMBRE", width=320)
        self.tree_sp.column("SP", width=550)
        self.tree_sp.column("ESTADO", width=100, anchor="center")

        self.tree_sp.pack(fill="both", expand=True)

        self.tree_sp.tag_configure("fila", background="white")

        self.tree_sp.bind("<Double-1>", self.editar_celda_sp)

        self.cargar_sps()

    # =====================================
    # CARGAR SPS
    # =====================================

    def cargar_sps(self):

        for row in self.tree_sp.get_children():
            self.tree_sp.delete(row)

        with sqlite3.connect(self.db_path) as conn:

            df = pd.read_sql(
                "SELECT NOMBRE_MOSTRAR,SP_ASOCIADO,ESTADO FROM CONFIG_CUBOS",
                conn
            )

        for fila in df.itertuples():
            self.tree_sp.insert(
                "",
                "end",
                values=(fila.NOMBRE_MOSTRAR, fila.SP_ASOCIADO, fila.ESTADO),
                tags=("fila",)
            )

    # =====================================
    # AGREGAR SP
    # =====================================

    def agregar_sp(self):

        nombre = self.sp_nombre.get()
        sp = self.sp_asociado.get()

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO CONFIG_CUBOS
                (NOMBRE_MOSTRAR,SP_ASOCIADO,ESTADO)
                VALUES (?, ?, 1)
            """, (nombre, sp))

            conn.commit()

        self.cargar_sps()

    # =====================================
    # ELIMINAR SP
    # =====================================

    def eliminar_sp(self):

        item = self.tree_sp.selection()

        if not item:
            return

        nombre = self.tree_sp.item(item)["values"][0]

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute(
                "DELETE FROM CONFIG_CUBOS WHERE NOMBRE_MOSTRAR=?",
                (nombre,)
            )

            conn.commit()

        self.cargar_sps()

    # =====================================
    # EDITAR CELDA SP
    # =====================================

    def editar_celda_sp(self, event):

        region = self.tree_sp.identify("region", event.x, event.y)

        if region != "cell":
            return

        row_id = self.tree_sp.identify_row(event.y)
        column = self.tree_sp.identify_column(event.x)

        x, y, width, height = self.tree_sp.bbox(row_id, column)

        valor_actual = self.tree_sp.set(row_id, column)

        entry = tb.Entry(self.tree_sp, font=("Segoe UI",12))
        entry.place(x=x, y=y, width=width, height=height)

        entry.insert(0, valor_actual)
        entry.focus()

        def guardar(event):

            nuevo_valor = entry.get()

            self.tree_sp.set(row_id, column, nuevo_valor)

            valores = self.tree_sp.item(row_id)["values"]

            with sqlite3.connect(self.db_path) as conn:

                cursor = conn.cursor()

                cursor.execute("""
                    UPDATE CONFIG_CUBOS
                    SET NOMBRE_MOSTRAR=?,
                        SP_ASOCIADO=?,
                        ESTADO=?
                    WHERE NOMBRE_MOSTRAR=?
                """,
                (valores[0], valores[1], valores[2], valor_actual))

                conn.commit()

            entry.destroy()

        entry.bind("<Return>", guardar)
        entry.bind("<FocusOut>", lambda e: entry.destroy())

    # =====================================
    # TAB USUARIOS
    # =====================================

    def crear_tab_users(self):

        frame_top = tb.Frame(self.tab_users)
        frame_top.pack(fill="x", pady=10)

        self.new_user = tb.Entry(frame_top, width=25)
        self.new_user.insert(0, "usuario")
        self.new_user.pack(side="left", padx=5)

        self.new_pass = tb.Entry(frame_top, width=25)
        self.new_pass.insert(0, "password")
        self.new_pass.pack(side="left", padx=5)

        tb.Button(frame_top,text="Crear Usuario",
                  bootstyle="success",
                  command=self.crear_usuario).pack(side="left", padx=5)

        tb.Button(frame_top,text="Habilitar / Deshabilitar",
                  bootstyle="warning",
                  command=self.toggle_usuario).pack(side="left", padx=5)

        tb.Button(frame_top,text="Recargar",
                  bootstyle="info",
                  command=self.cargar_users).pack(side="left", padx=5)

        columnas = ("USUARIO","ROL","ESTADO")

        self.tree_users = tb.Treeview(
            self.tab_users,
            columns=columnas,
            show="headings",
            height=20
        )

        self.tree_users.heading("USUARIO", text="USUARIO")
        self.tree_users.heading("ROL", text="ROL")
        self.tree_users.heading("ESTADO", text="ESTADO")

        self.tree_users.column("USUARIO", width=280)
        self.tree_users.column("ROL", width=250)
        self.tree_users.column("ESTADO", width=120, anchor="center")

        self.tree_users.pack(fill="both", expand=True)

        self.tree_users.tag_configure("fila", background="white")

        self.tree_users.bind("<Double-1>", self.editar_celda_user)

        self.cargar_users()

    # =====================================
    # CARGAR USUARIOS
    # =====================================

    def cargar_users(self):

        for row in self.tree_users.get_children():
            self.tree_users.delete(row)

        with sqlite3.connect(self.db_path) as conn:

            df = pd.read_sql(
                "SELECT USUARIO,TIPO_PERMISO_ACCES,ESTADO FROM CREDENCIALES",
                conn
            )

        for fila in df.itertuples():

            estado = "Activo" if fila.ESTADO == 1 else "Deshabilitado"

            self.tree_users.insert(
                "",
                "end",
                values=(fila.USUARIO, fila.TIPO_PERMISO_ACCES, estado),
                tags=("fila",)
            )

    # =====================================
    # CREAR USUARIO
    # =====================================

    def crear_usuario(self):

        u = self.new_user.get()
        p = self.new_pass.get()

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO CREDENCIALES
                (TIPO_PERMISO_ACCES,ID_ROLE,LINK,USUARIO,PASSWORD,TIPO,ESTADO)
                VALUES
                ('OPERADOR',2,'NA',?,?, 'USER',1)
            """, (u, p))

            conn.commit()

        self.cargar_users()

    # =====================================
    # ACTIVAR / DESACTIVAR USUARIO
    # =====================================

    def toggle_usuario(self):

        item = self.tree_users.selection()

        if not item:
            return

        usuario = self.tree_users.item(item)["values"][0]

        with sqlite3.connect(self.db_path) as conn:

            cursor = conn.cursor()

            cursor.execute("""
                UPDATE CREDENCIALES
                SET ESTADO = CASE WHEN ESTADO=1 THEN 0 ELSE 1 END
                WHERE USUARIO=?
            """, (usuario,))

            conn.commit()

        self.cargar_users()

    # =====================================
    # EDITAR CELDA USUARIO
    # =====================================

    def editar_celda_user(self, event):

        region = self.tree_users.identify("region", event.x, event.y)

        if region != "cell":
            return

        row_id = self.tree_users.identify_row(event.y)
        column = self.tree_users.identify_column(event.x)

        x, y, width, height = self.tree_users.bbox(row_id, column)

        valor_actual = self.tree_users.set(row_id, column)

        entry = tb.Entry(self.tree_users, font=("Segoe UI",12))
        entry.place(x=x, y=y, width=width, height=height)

        entry.insert(0, valor_actual)
        entry.focus()

        def guardar(event):

            nuevo_valor = entry.get()

            self.tree_users.set(row_id, column, nuevo_valor)

            valores = self.tree_users.item(row_id)["values"]

            with sqlite3.connect(self.db_path) as conn:

                cursor = conn.cursor()

                cursor.execute("""
                    UPDATE CREDENCIALES
                    SET TIPO_PERMISO_ACCES=?,
                        ESTADO=?
                    WHERE USUARIO=?
                """,
                (valores[1], 1 if valores[2] == "Activo" else 0, valores[0]))

                conn.commit()

            entry.destroy()

        entry.bind("<Return>", guardar)
        entry.bind("<FocusOut>", lambda e: entry.destroy())
















# =====================================

if __name__ == "__main__":
    multiprocessing.freeze_support()
    multiprocessing.set_start_method("spawn", force=True)

    app = VentanaLogin()
    app.mainloop()
