#!/usr/bin/env python
# src/interfaz_grafica.py

import os
import sys
import json
import logging
import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import subprocess
import pg8000
import pandas as pd
from tkcalendar import DateEntry
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from sklearn.decomposition import PCA
import threading
import socket
import json
import os


class ToolTip:
    """
    Crea una ayuda emergente (tooltip) para un widget dado.
    """

    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tipwin = None
        widget.bind("<Enter>", self.show)
        widget.bind("<Leave>", self.hide)

    def show(self, _event):
        if self.tipwin or not self.text:
            return
        try:
            if hasattr(self.widget, "bbox"):
                # Solo usar bbox("insert") en widgets de texto
                if self.widget.winfo_class() in ("Text", "Entry"):
                    x, y, cx, cy = self.widget.bbox("insert")
                else:
                    # Para Treeview, Combobox u otros: valores seguros
                    x, y, cx, cy = (0, 0, 0, 0)
            else:
                x, y, cx, cy = (0, 0, 0, 0)
        except Exception:
            # Si bbox falla, devolvemos valores seguros
            x, y, cx, cy = (0, 0, 0, 0)

        x += self.widget.winfo_rootx() + 20
        y += self.widget.winfo_rooty() + 20
        self.tipwin = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            font=("Arial", "8", "normal"),
        )
        label.pack(ipadx=1)

    def hide(self, _event):
        if self.tipwin:
            self.tipwin.destroy()
            self.tipwin = None
# ————————————— Bloque: Configuración global —————————————
# Variables de configuración de la BD, colores gráficos, archivo de settings y configuración por defecto
DB_CONFIG = {
    "host": "ep-lucky-morning-adafnn5y-pooler.c-2.us-east-1.aws.neon.tech",
    "user": "neondb_owner",
    "password": "npg_pgxVl1e3BMqH",
    "database": "neondb",  # asegúrate que el nombre de la base sea correcto
    "port": 5432
}

COLOR_BG = "#2c3e50"  # Color de fondo
COLOR_BT = "#3498db"  # Color de botones
COLOR_FG = "#ffffff"  # Color de fuente
SETTINGS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "settings.json"))
DEFAULT_SETTINGS = {"cycles": [2, 3, 4, 5], "ppm_factor": 1.0, "alert_threshold": 0.5}
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()


class Aplicacion(tk.Tk):
    """
    Clase principal que representa la interfaz gráfica del Sistema de Monitoreo Electroquímico.
    Integra módulos para cargar datos, realizar consultas, visualizar y exportar gráficos y reportes.
    """

    # ————— Bloque: Inicialización y carga de ajustes —————
    def __init__(self):
        """
        Inicializa la ventana principal, carga ajustes, configura estilo, menú y pestañas.
        También carga las sesiones iniciales.
        """
        super().__init__()
        self.title("Sistema de Monitoreo Electroquímico")
        self.geometry("1400x900")
        self.configure(bg=COLOR_BG)

        self.current_data = None  # DataFrame con datos de la sesión actual
        self.session_info = {}  # Diccionario con metadatos de la sesión
        self.ppm_df = None  # DataFrame para la tabla de estimaciones ppm
        self.settings = self.load_settings()  # Carga o crea el archivo settings.json

        # Configurar estilo de la interfaz, crear menú y pestañas
        self.setup_style()
        self.create_menu()
        self.create_tabs()
        self.load_sessions()

    def load_file(self):
        """
        Llama al método de selección de archivo para cargar .pssession.
        """
        return self.seleccionar_archivo()

    def load_settings(self):
        """
        Carga los ajustes del archivo settings.json.
        Si no existe o hay error, utiliza los ajustes por defecto.

        Returns:
            dict: Diccionario de ajustes.
        """
        if not os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "w") as f:
                json.dump(DEFAULT_SETTINGS, f, indent=2)
            return DEFAULT_SETTINGS.copy()
        try:
            return json.load(open(SETTINGS_FILE))
        except:
            log.warning("No se pudo leer settings.json; usando valores por defecto")
            return DEFAULT_SETTINGS.copy()

    def save_settings(self):
        """
        Guarda los ajustes actuales en el archivo settings.json.
        """
        with open(SETTINGS_FILE, "w") as f:
            json.dump(self.settings, f, indent=2)
        log.info("Settings guardados: %s", self.settings)

    # ————— Bloque: Estilos —————
    def setup_style(self):
        """
        Configura el estilo visual de la aplicación (colores, fuentes, temas) utilizando ttk.Style.
        """
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=COLOR_BG)
        s.configure("TButton", background=COLOR_BT, foreground=COLOR_FG, font=("Arial", 12, "bold"))
        s.map("TButton", background=[("active", "#2980b9")])
        s.configure("TLabel", background=COLOR_BG, foreground=COLOR_FG, font=("Arial", 11))
        s.configure("Treeview", background="#34495e", fieldbackground="#34495e", foreground=COLOR_FG)
        s.configure("TNotebook", background=COLOR_BG)
        s.configure("TNotebook.Tab", background="#7f8c8d", foreground=COLOR_FG, font=("Arial", 10, "bold"))

    # ————— Bloque: Menú principal —————
    def create_menu(self):
        """
        Crea el menú principal de la aplicación con opciones de archivo y ayuda.
        """
        m = tk.Menu(self)
        f = tk.Menu(m, tearoff=0)
        f.add_separator()
        f.add_command(label="Salir", command=self.quit)
        m.add_cascade(label="Archivo", menu=f)
        h = tk.Menu(m, tearoff=0)
        h.add_command(
            label="Acerca de", command=lambda: messagebox.showinfo("Acerca de", "Sistema Monitoreo Electroquímico v2.0")
        )
        m.add_cascade(label="Ayuda", menu=h)
        self.config(menu=m)

    # ————— Bloque: Creación de pestañas —————
    def create_tabs(self):
        """
        Crea las pestañas principales de la interfaz para diferentes módulos:
        Cargar Datos, Consultas, Detalle, Curvas, PCA y ppm.
        """
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)
        self.build_load_tab(nb)
        self.build_query_tab(nb)
        self.build_detail_tab(nb)
        self.build_curve_tab(nb)
        self.build_pca_tab(nb)
        self.build_ppm_tab(nb)
        self.build_iot_tab(nb)
        

    # ————— Bloque: Pestaña “Cargar Datos” —————
    def build_load_tab(self, parent):
        """
        Configura la pestaña de carga de datos (.pssession).

        Args:
            parent (ttk.Notebook): Notebook padre donde se añade la pestaña.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="📤 Cargar Datos")
        ttk.Button(f, text="Seleccionar Archivo .pssession", command=self.load_file).pack(pady=20)
        self.log_text = tk.Text(f, height=8, bg="#34495e", fg="white", font=("Courier", 10))
        self.log_text.pack(fill="x", padx=10, pady=10)

    # ---------------------- BLOQUE 2 REFACTORIZADO CON DEBUGS: MENÚ Y CONSULTAS ----------------------

    # ——— Bloque 2.1 ———
    def show_settings_alternative(self):
        """
        Ventana de Configuraciones Alternativas.
        Permite al usuario ajustar el umbral de alerta (ppm).
        Debug: imprime entrada y errores.
        """
        print("[DEBUG] show_settings_alternative() invoked")
        settings_window = tk.Toplevel(self)
        settings_window.title("Configuraciones")
        settings_window.geometry("400x300")
        settings_window.transient(self)
        settings_window.grab_set()

        main_frame = ttk.Frame(settings_window)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)

        ttk.Label(main_frame, text="Configuraciones", font=("Arial", 14, "bold")).pack(pady=(0, 20))

        threshold_frame = ttk.Frame(main_frame)
        threshold_frame.pack(fill="x", pady=5)
        ttk.Label(threshold_frame, text="Umbral de Alerta (ppm):").pack(side="left")
        threshold_var = tk.StringVar(value=str(self.settings.get("alert_threshold", 100)))
        threshold_entry = ttk.Entry(threshold_frame, textvariable=threshold_var, width=10)
        threshold_entry.pack(side="right")

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(side="bottom", fill="x", pady=(20, 0))

        def save_settings():
            print("[DEBUG] save_settings() invoked with value:", threshold_var.get())
            try:
                new_thr = float(threshold_var.get())
                self.settings["alert_threshold"] = new_thr
                if hasattr(self, "threshold_entry"):
                    self.threshold_entry.delete(0, "end")
                    self.threshold_entry.insert(0, str(new_thr))
                messagebox.showinfo("Éxito", "Umbral guardado correctamente")
                settings_window.destroy()
            except ValueError:
                print("[DEBUG] save_settings() ValueError: invalid float")
                messagebox.showerror("Error", "El umbral debe ser un número válido")

        ttk.Button(button_frame, text="Guardar", command=save_settings).pack(side="right", padx=(5, 0))
        ttk.Button(button_frame, text="Cancelar", command=settings_window.destroy).pack(side="right")

    # ——— Bloque 2.2 ———
    def build_query_tab(self, parent):
        """
        Armado de la pestaña 'Consultas', llamando a los sub-bloques:
          2.2.1 Vista general
          2.2.2 Filtros básicos
          2.2.3 Tabla de resultados
          2.2.4 Detalles técnicos
        """
        print("[DEBUG] build_query_tab() invoked")
        tab = ttk.Frame(parent)
        parent.add(tab, text="🔍 Consultas")

        self._create_overview_panel(tab)  # 2.2.1
        self._create_filters_panel(tab)  # 2.2.2
        self._create_results_table(tab)  # 2.2.3
        self._create_meta_panel(tab)  # 2.2.4

        self.load_devices()
        self.update_overview()
        self.set_default_date_range()
        self.query_sessions()

    # ——— Bloque 2.2.1 ———
    def _create_overview_panel(self, parent):
        """
        Panel superior de estadísticas generales.
        """
        print("[DEBUG] _create_overview_panel() invoked")
        frame = ttk.LabelFrame(parent, text="Vista General de Datos")
        frame.pack(fill="x", padx=10, pady=5)
        container = ttk.Frame(frame)
        container.pack(fill="x", padx=10, pady=10)

        fields = [
            ("total_sessions", "Total de Sesiones: --"),
            ("total_measurements", "Total de Mediciones: --"),
            ("avg_ppm", "PPM Promedio: --"),
            ("max_ppm", "PPM Máximo: --"),
            ("alert_count", "Alertas Activas: --"),
            ("last_update", "Última Actualización: --"),
        ]
        self.overview_labels = {}
        for i, (key, text) in enumerate(fields):
            r, c = divmod(i, 3)
            lbl = ttk.Label(container, text=text, font=("Arial", 10))
            lbl.grid(row=r, column=c, sticky="w", padx=20, pady=5)
            self.overview_labels[key] = lbl

        ttk.Button(
            container,
            text="🔄 Actualizar",
            command=lambda: (print("[DEBUG] Click: actualizar vista general"), self.update_overview()),
        ).grid(row=2, column=0, columnspan=3, pady=10)

    # ——— Bloque 2.2.2 ———
    def _create_filters_panel(self, parent):
        """
        Panel de filtros básicos por ID, rango de fechas y dispositivo.
        """
        print("[DEBUG] _create_filters_panel() invoked")
        frame = ttk.LabelFrame(parent, text="Filtros de Búsqueda")
        frame.pack(fill="x", padx=10, pady=5)
        container = ttk.Frame(frame)
        container.pack(fill="x", padx=10, pady=10)

        # ID Sesión
        ttk.Label(container, text="ID Sesión:").grid(row=0, column=0, sticky="e", padx=5)
        self.id_entry = ttk.Entry(container, width=10)
        self.id_entry.grid(row=0, column=1, padx=5)
        ToolTip(self.id_entry, "Introduce el ID de la sesión para filtrar los resultados")

        # Rango de fechas
        ttk.Label(container, text="Fecha Inicio:").grid(row=0, column=2, sticky="e", padx=5)
        self.date_start = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_start.grid(row=0, column=3, padx=5)
        ToolTip(self.date_start, "Selecciona la fecha de inicio del rango de búsqueda")

        ttk.Label(container, text="Fecha Fin:").grid(row=0, column=4, sticky="e", padx=5)
        self.date_end = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_end.grid(row=0, column=5, padx=5)
        ToolTip(self.date_end, "Selecciona la fecha de fin del rango de búsqueda")

        # Dispositivo (nuevo combobox)
        ttk.Label(container, text="Dispositivo:").grid(row=1, column=0, sticky="e", padx=5, pady=(10, 0))
        self.device_combobox = ttk.Combobox(container, state="readonly", width=12)
        self.device_combobox.grid(row=1, column=1, padx=5, pady=(10, 0))
        self.device_combobox["values"] = ["— Todos —"]
        self.device_combobox.current(0)
        ToolTip(self.device_combobox, "Filtra los resultados por el número de serie del dispositivo")

        # Botones de acción
        btns = ttk.Frame(container)
        btns.grid(row=2, column=0, columnspan=6, pady=(10, 0))

        btn_search = ttk.Button(
            btns, text="🔍 Buscar", command=lambda: (print("[DEBUG] Click: buscar sesiones"), self.query_sessions())
        )
        btn_search.pack(side="left", padx=5)
        ToolTip(btn_search, "Ejecuta la búsqueda con los filtros seleccionados")

        btn_clear = ttk.Button(
            btns, text="🗑️ Limpiar", command=lambda: (print("[DEBUG] Click: limpiar filtros"), self.clear_filters())
        )
        btn_clear.pack(side="left", padx=5)
        ToolTip(btn_clear, "Limpia todos los filtros y restablece valores por defecto")

        btn_last7 = ttk.Button(
            btns,
            text="📅 Últimos 7d",
            command=lambda: (print("[DEBUG] Click: últimos 7 días"), self.set_default_date_range()),
        )
        btn_last7.pack(side="left", padx=5)
        ToolTip(btn_last7, "Establece el rango de fechas a los últimos siete días")

    # ——— Bloque 2.2.3 EXTENDIDO con Tooltips ———
    def _create_results_table(self, parent):
        """
        Crea la tabla de resultados con la nueva columna 'Contaminantes'
        y enlaza el evento de selección.
        """
        print("[DEBUG] _create_results_table() invoked")
        frame = ttk.LabelFrame(parent, text="Resultados de Búsqueda")
        frame.pack(fill="both", expand=True, padx=10, pady=5)
        table_frame = ttk.Frame(frame)
        table_frame.pack(fill="both", expand=True, padx=5, pady=5)

        cols = ("ID", "Archivo", "Fecha", "Dispositivo", "Curvas", "Estado", "Máx. ppm", "Contaminantes")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        for col in cols:
            self.tree.heading(col, text=col)
            width = 120 if col == "Contaminantes" else 100
            self.tree.column(col, width=width, anchor="center")

        v_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        h_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
        v_scroll.pack(side="right", fill="y")
        h_scroll.pack(side="bottom", fill="x")

        self.tree.pack(fill="both", expand=True)
        ToolTip(self.tree, "Tabla con las sesiones encontradas; selecciona una para ver detalles.")

        self.tree.tag_configure("alert", background="#ffebee", foreground="#c62828")
        self.tree.tag_configure("safe", background="#e8f5e9", foreground="#2e7d32")
        self.tree.bind("<<TreeviewSelect>>", lambda ev: self.on_session_select())
        ToolTip(self.tree, "Al hacer clic en una fila, se mostrarán los detalles técnicos abajo.")

    # ——— Bloque 2.2.4 con Tooltips ———
    def _create_meta_panel(self, parent):
        """
        Panel de detalles técnicos de la sesión seleccionada.
        """
        print("[DEBUG] _create_meta_panel() invoked")
        frame = ttk.LabelFrame(parent, text="Detalles Técnicos de la Sesión Seleccionada")
        frame.pack(fill="x", padx=10, pady=5)
        meta_content = ttk.Frame(frame)
        meta_content.pack(fill="x", padx=10, pady=10)

        # Campos disponibles según esquema de BD
        fields = [
            ("scan_rate", "Velocidad de Escaneo: --"),
            ("start_potential", "Potencial Inicial: --"),
            ("end_potential", "Potencial Final: --"),
            ("software_version", "Versión Software: --"),
        ]
        self.meta_labels = {}
        for i, (key, text) in enumerate(fields):
            r, c = divmod(i, 2)
            lbl = ttk.Label(meta_content, text=text, font=("Arial", 9))
            lbl.grid(row=r, column=c, sticky="w", padx=15, pady=2)
            self.meta_labels[key] = lbl
            ToolTip(lbl, f"Muestra el valor de '{key}' de la sesión seleccionada")
    # ——— Bloque 2.3 EXTENDIDO (metales contaminantes) ———
    def query_sessions(self):
        """
        Ejecuta la consulta con filtros (ID, fechas, dispositivo)
        y actualiza la tabla con colores y estados legibles.
        """
        log.debug("query_sessions() invoked")

        # 1) Filtro por ID de sesión
        sid_text = self.id_entry.get().strip()
        try:
            session_id = int(sid_text) if sid_text else None
        except ValueError:
            log.debug(f"ID inválido: '{sid_text}' – ignorando filtro de ID.")
            session_id = None

        # 2) Fechas y filtros de dispositivo
        start_date = self.date_start.get_date().strftime("%Y-%m-%d")
        end_date = self.date_end.get_date().strftime("%Y-%m-%d")

        device = self.device_combobox.get()
        use_device_filter = device and device != "— Todos —"

        params = [start_date, end_date]
        if session_id is not None:
            params.append(session_id)
        if use_device_filter:
            params.append(device)

        # 3) Consulta SQL principal
        sql = """
            SELECT
              s.id,
              s.filename,
              s.loaded_at::date AS fecha,
              m.device_serial AS dispositivo,
              m.curve_count AS curvas,
              CASE
                WHEN m.classification_group = 1 THEN '⚠️ CONTAMINADA'
                WHEN m.classification_group = 2 THEN '🟡 ANÓMALA'
                ELSE '✅ SEGURA'
              END AS estado,
              COALESCE(ROUND(m.contamination_level::numeric, 2), 0) AS max_ppm,
              m.title AS contaminantes
            FROM sessions s
            JOIN measurements m
              ON s.id = m.session_id
            WHERE s.loaded_at::date BETWEEN %s AND %s
        """
        if session_id is not None:
            sql += "  AND s.id = %s\n"
        if use_device_filter:
            sql += "  AND m.device_serial = %s\n"

        sql += "ORDER BY s.loaded_at DESC"

        log.debug(f"Params tuple: {params}")
        log.debug(f"SQL:\n{sql}")

        # 4) Ejecutar la consulta
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error(f"Error en query_sessions: {e}")
            messagebox.showerror("Error en consulta", f"No se pudo ejecutar la consulta:\n{e}")
            return

        # 5) Limpiar y poblar la tabla
        self.tree.delete(*self.tree.get_children())

        if not rows:
            self.tree.insert("", "end", values=("--", "Sin resultados", "--", "--", "--", "--", "--", "--"))
            return

        for r in rows:
            estado_texto = str(r[5]).upper()
            if "CONTAMINADA" in estado_texto:
                tag = "alert"
            elif "ANÓMALA" in estado_texto:
                tag = "warning"
            else:
                tag = "safe"

            self.tree.insert("", "end", values=r, tags=(tag,))

        # 6) Estilos visuales
        self.tree.tag_configure("alert", background="#ffebee", foreground="#c62828")    # rojo claro
        self.tree.tag_configure("warning", background="#fff9c4", foreground="#f57f17")  # amarillo claro
        self.tree.tag_configure("safe", background="#e8f5e9", foreground="#2e7d32")     # verde claro

        log.debug(f"✅ {len(rows)} filas actualizadas en la tabla de resultados")

        # 7) Refrescar estadísticas
        self.update_overview()


    # ——— Bloque 2.5: load_devices (mejorado) ———
    def load_devices(self):
        """
        Consulta la base de datos para cargar los seriales de dispositivos
        y asignarlos al combobox, siempre incluyendo la opción “— Todos —”.
        Además, enlaza el evento de selección para actualizar la consulta.
        """
        print("[DEBUG] load_devices() invoked")
        if not hasattr(self, "device_combobox"):
            print("[DEBUG] load_devices: no existe device_combobox, skip.")
            return

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT device_serial
                FROM measurements
                WHERE device_serial IS NOT NULL
                ORDER BY device_serial
            """
            )
            vals = [row[0] for row in cur.fetchall()]
            conn.close()

            # Siempre incluir “— Todos —” al inicio
            options = ["— Todos —"] + vals if vals else ["— Todos —"]
            print(f"[DEBUG] load_devices: valores obtenidos: {options}")
            self.device_combobox["values"] = options
            self.device_combobox.current(0)

            # Enlazar la selección para refrescar consultas
            self.device_combobox.bind(
                "<<ComboboxSelected>>",
                lambda ev: (
                    print(f"[DEBUG] Dispositivo seleccionado: {self.device_combobox.get()}"),
                    self.query_sessions(),
                ),
            )

        except Exception as e:
            print(f"[DEBUG] load_devices Error: {e}")
            messagebox.showerror("Error BD", f"Error cargando dispositivos:\n{e}")

    # ——— Bloque 2.6: update_overview ———
    def update_overview(self):
        """
        Actualiza la vista general mostrando estadísticas globales.
        Debug: imprime cada paso y captura excepciones.
        """
        print("[DEBUG] update_overview() invoked")
        # Si no hay overview_labels, salimos
        if not hasattr(self, "overview_labels"):
            print("[DEBUG] update_overview: no existe overview_labels, skip.")
            return

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()

            queries = {
                "total_sessions": "SELECT COUNT(*) FROM sessions",
                "total_measurements": "SELECT COUNT(*) FROM measurements",
                "avg_ppm": "SELECT ROUND(AVG(contamination_level)::numeric, 2) FROM measurements",
                "max_ppm": "SELECT ROUND(MAX(contamination_level)::numeric, 2) FROM measurements",
                "alert_count": "SELECT COUNT(*) FROM measurements WHERE contamination_level > %s",
                "last_update": "SELECT MAX(loaded_at) FROM sessions",
            }

            stats = {}
            for key, sql in queries.items():
                if key == "alert_count":
                    cur.execute(sql, (self.settings["alert_threshold"],))
                else:
                    cur.execute(sql)
                stats[key] = cur.fetchone()[0]

            conn.close()
            print(f"[DEBUG] update_overview: stats fetched: {stats}")

            # Actualizar labels
            self.overview_labels["total_sessions"].config(text=f"Total de Sesiones: {stats['total_sessions']}")
            self.overview_labels["total_measurements"].config(
                text=f"Total de Mediciones: {stats['total_measurements']}"
            )
            self.overview_labels["avg_ppm"].config(text=f"PPM Promedio: {stats['avg_ppm']}")
            self.overview_labels["max_ppm"].config(text=f"PPM Máximo: {stats['max_ppm']}")
            self.overview_labels["alert_count"].config(text=f"Alertas Activas: {stats['alert_count']}")

            last = stats["last_update"]
            if last:
                formatted = last.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last, "strftime") else str(last)
                text = f"Última Actualización: {formatted}"
            else:
                text = "Última Actualización: --"
            self.overview_labels["last_update"].config(text=text)

        except Exception as e:
            print(f"[DEBUG] update_overview Error: {e}")
            messagebox.showerror("Error", f"Error actualizando vista general:\n{e}")
            # Reset labels en caso de fallo
            for lbl in getattr(self, "overview_labels", {}).values():
                lbl.config(text="--")

    # ——— Bloque 2.7: set_default_date_range ———
    def set_default_date_range(self):
        """
        Establece el rango de fechas a los últimos 7 días y dispara la consulta.
        Debug: imprime los valores de fecha establecidos.
        """
        print("[DEBUG] set_default_date_range() invoked")
        # Verificar que existan los datepickers
        if not hasattr(self, "date_start") or not hasattr(self, "date_end"):
            print("[DEBUG] set_default_date_range: no existen date_start/date_end, skip.")
            return

        try:
            today = datetime.date.today()
            last7 = today - datetime.timedelta(days=7)
            self.date_start.set_date(last7)
            self.date_end.set_date(today)
            print(f"[DEBUG] set_default_date_range: date_start={last7}, date_end={today}")
            # Ejecutar la consulta con el nuevo rango
            self.query_sessions()
        except Exception as e:
            print(f"[DEBUG] set_default_date_range Error: {e}")
            messagebox.showerror("Error", f"Error estableciendo rango de fechas:\n{e}")

    # ——— Bloque: Pestaña “Detalle Sesión” ———
    def build_detail_tab(self, parent):
        """
        Crea la pestaña 'Detalle Sesión' donde se muestra un área de texto
        para información detallada de la sesión.

        Args:
            parent (ttk.Notebook): Notebook donde se añade la pestaña.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="📝 Detalle Sesión")
        self.txt_detail = tk.Text(f, wrap="word", bg="#34495e", fg="white", font=("Arial", 10))
        self.txt_detail.pack(fill="both", expand=True, padx=10, pady=10)
        ToolTip(self.txt_detail, "Aquí se muestra la información detallada (JSON) de la sesión seleccionada")

    # ——— Bloque: Pestaña “Curvas” ———
    def build_curve_tab(self, parent):
        """
        Configura la pestaña 'Curvas' para visualizar curvas individuales y promedio.

        Args:
            parent (ttk.Notebook): Notebook donde se añade la pestaña.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="📊 Curvas")
        frm = ttk.Frame(f)
        frm.pack(fill="x", padx=10, pady=8)

        ttk.Label(frm, text="Índice medida:").pack(side="left", padx=5)
        ToolTip(frm.winfo_children()[-1], "Selecciona el índice de la medición para graficar la curva correspondiente")

        self.cmb_curve = ttk.Combobox(frm, state="readonly", width=8)
        self.cmb_curve.pack(side="left", padx=5)
        ToolTip(self.cmb_curve, "Desplegable para elegir cuál de las mediciones graficar")
        self.cmb_curve.bind("<<ComboboxSelected>>", lambda e: self.show_curve())

        self.fig_curve, self.ax_curve = plt.subplots(figsize=(9, 5), facecolor=COLOR_BG)
        self.ax_curve.set_facecolor(COLOR_BG)
        self.ax_curve.tick_params(colors="white")
        self.ax_curve.grid(True, color="#5d6d7e")
        self.canvas_curve = FigureCanvasTkAgg(self.fig_curve, master=f)
        self.canvas_curve.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        ToolTip(self.canvas_curve.get_tk_widget(), "Gráfica de corriente vs potencial para la curva seleccionada")

        btn_export_curve = ttk.Button(f, text="Exportar PNG", command=lambda: self.export_figure(self.fig_curve))
        btn_export_curve.pack(side="right", padx=10, pady=5)
        ToolTip(btn_export_curve, "Exporta la gráfica de curvas como imagen PNG")


    # ——— Bloque: Pestaña “ppm” (ahora “Clasificación”) ———
    def build_ppm_tab(self, parent):
        """
        Configura la pestaña 'ppm' (ahora 'Clasificación') para mostrar
        grupo y nivel de contaminación y permitir su exportación.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="🗂 Clasificación")

        # Botón para refrescar la vista de clasificación
        btn_show_ppm = ttk.Button(
            f,
            text="Mostrar Clasificación",
            command=self.show_classification
        )
        btn_show_ppm.pack(pady=8)
        ToolTip(btn_show_ppm, "Dibuja la clasificación y nivel de contaminación")

        # Reutilizamos self.tree_ppm para la tabla, ahora con dos columnas:
        cols = ("Grupo", "Nivel (%)")
        self.tree_ppm = ttk.Treeview(
            f,
            columns=cols,
            show="headings",
            height=8
        )
        for c in cols:
            self.tree_ppm.heading(c, text=c)
            self.tree_ppm.column(c, anchor="center")
        self.tree_ppm.pack(fill="both", expand=True, padx=10, pady=10)
        ToolTip(self.tree_ppm, "Tabla con grupo de clasificación y nivel de contaminación")

        # Botón para exportar la tabla de clasificación
        btn_export_ppm = ttk.Button(
            f,
            text="Exportar Clasificación",
            command=self.export_classification
        )
        btn_export_ppm.pack(side="right", padx=10, pady=5)
        ToolTip(btn_export_ppm, "Exporta la tabla de clasificación como CSV")
     # ——— Bloque: Pestaña “IoT / Comunicación” ———
    def build_iot_tab(self, parent):
        """
        Crea la pestaña para control del servidor IoT, conexión remota y envío de archivos.
        Permite iniciar/detener servidor, probar conexión y transferir archivos.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="🌐 IoT / Comunicación")

        ttk.Label(f, text="Centro de Control IoT", font=("Arial", 14, "bold")).pack(pady=10)

        # ==============================
        # CONFIGURACIÓN
        # ==============================
        frame_conf = ttk.LabelFrame(f, text="Configuración del Servidor / Cliente")
        frame_conf.pack(fill="x", padx=10, pady=10)

        ttk.Label(frame_conf, text="IP del servidor remoto:").grid(row=0, column=0, padx=5, sticky="e")
        self.iot_ip_var = tk.StringVar(value="")
        ttk.Entry(frame_conf, textvariable=self.iot_ip_var, width=18).grid(row=0, column=1, padx=5)

        ttk.Label(frame_conf, text="Puerto:").grid(row=0, column=2, padx=5, sticky="e")
        self.iot_port_var = tk.IntVar(value=5000)
        ttk.Entry(frame_conf, textvariable=self.iot_port_var, width=8).grid(row=0, column=3, padx=5)

        # ==============================
        # CONTROLES DE SERVIDOR LOCAL
        # ==============================
        frame_srv = ttk.LabelFrame(f, text="Servidor IoT Local")
        frame_srv.pack(fill="x", padx=10, pady=5)

        self.server_running = False
        self.server_thread = None

        ttk.Button(frame_srv, text="🚀 Iniciar Servidor", command=self.start_iot_server).grid(row=0, column=0, padx=5, pady=5)
        ttk.Button(frame_srv, text="🛑 Detener Servidor", command=self.stop_iot_server).grid(row=0, column=1, padx=5, pady=5)

        # ==============================
        # FUNCIONES DE CLIENTE
        # ==============================
        frame_cli = ttk.LabelFrame(f, text="Cliente IoT (modo remoto)")
        frame_cli.pack(fill="x", padx=10, pady=5)

        ttk.Button(frame_cli, text="🔌 Probar Conexión", command=self.test_iot_connection).grid(row=0, column=0, padx=5)
        ttk.Button(frame_cli, text="📤 Enviar Archivo", command=self.send_iot_file).grid(row=0, column=1, padx=5)

        # ==============================
        # PROGRESO + LOGS
        # ==============================
        ttk.Label(f, text="Progreso de envío:").pack(pady=(15, 5))
        self.iot_progress = ttk.Progressbar(f, length=500, mode="determinate")
        self.iot_progress.pack(pady=5)

        self.iot_log = tk.Text(f, height=12, bg="#1e272e", fg="white", font=("Courier", 10))
        self.iot_log.pack(fill="x", padx=10, pady=10)
        ToolTip(self.iot_log, "Registro detallado de eventos IoT")

        # ==============================
        # SECCIÓN DE AYUDA
        # ==============================
        ttk.Label(f, text="ℹ️ Ayuda rápida:", font=("Arial", 11, "bold")).pack(pady=(10, 2))
        help_text = (
            "1️⃣ Para recibir archivos, inicia el servidor en el dispositivo de destino.\n"
            "2️⃣ En el otro dispositivo, introduce la IP del servidor (ver más abajo).\n"
            "3️⃣ Usa 'Probar Conexión' para verificar.\n"
            "4️⃣ Si funciona, selecciona un archivo y presiona 'Enviar Archivo'.\n\n"
            "💡 Para obtener la IP en el dispositivo servidor (Linux/Mac):\n"
            "   👉 Ejecuta en la terminal: ip a | grep inet\n"
            "   Ejemplo: inet 192.168.0.45\n\n"
            "💡 En Windows:\n"
            "   👉 Abre CMD y escribe: ipconfig\n"
            "   Busca 'Dirección IPv4'.\n"
        )
        help_label = tk.Text(f, height=8, wrap="word", bg="#2c3e50", fg="#ecf0f1", font=("Arial", 9))
        help_label.insert("1.0", help_text)
        help_label.config(state="disabled")
        help_label.pack(fill="x", padx=15, pady=(0, 10))

    # ==============================
    # FUNCIONES AUXILIARES IoT
    # ==============================
    def log_iot(self, msg):
        """Agrega texto a la consola IoT."""
        self.iot_log.insert("end", msg + "\n")
        self.iot_log.see("end")
        log.info("[IoT] " + msg)

    def start_iot_server(self):
        """Inicia el servidor IoT en un hilo separado."""
        if self.server_running:
            self.log_iot("⚠️ El servidor ya está en ejecución.")
            return

        def server_loop():
            # Imports locales para evitar depender del scope global si pegas este bloque
            import sys
            import importlib
            import threading
            import hashlib
            import socket
            import json
            import os

            # Asegurar que la raíz del proyecto esté en sys.path para que `from src...` funcione
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            if project_root not in sys.path:
                sys.path.insert(0, project_root)

            host = "0.0.0.0"
            port = self.iot_port_var.get()
            buffer_size = 4096
            dest_dir = os.path.join(os.path.dirname(__file__), "..", "archivos_recibidos")
            os.makedirs(dest_dir, exist_ok=True)

            self.log_iot(f"🌐 Servidor IoT escuchando en {host}:{port}")
            self.server_running = True

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                server.bind((host, port))
                server.listen(5)
                server.settimeout(1)

                while self.server_running:
                    try:
                        conn, addr = server.accept()
                        self.log_iot(f"📡 Conexión desde {addr}")
                        with conn:
                            header_data = b""
                            while not header_data.endswith(b"\n"):
                                try:
                                    chunk = conn.recv(1)
                                except Exception as e:
                                    self.log_iot(f"❌ Error leyendo socket: {e}")
                                    break
                                if not chunk:
                                    break
                                header_data += chunk

                            if not header_data:
                                self.log_iot("⚠️ Conexión vacía.")
                                continue

                            header_text = header_data.decode(errors="replace").strip()

                            # Manejar pings simples y otros mensajes no JSON sin romper
                            if header_text.lower() == "ping" or header_text.lower() == "ping\n":
                                self.log_iot(f"📡 Ping recibido desde {addr}")
                                try:
                                    conn.sendall(b"PONG\n")
                                except Exception:
                                    pass
                                continue

                            # Intentar parsear JSON; si falla, responder y continuar
                            try:
                                header = json.loads(header_text)
                            except Exception as e:
                                self.log_iot(f"❌ Encabezado inválido (no JSON): {header_text!r} - {e}")
                                try:
                                    conn.sendall(b"ERR_INVALID_HEADER\n")
                                except Exception:
                                    pass
                                continue

                            # Validar keys mínimas
                            if not all(k in header for k in ("filename", "size", "checksum")):
                                self.log_iot(f"❌ Encabezado incompleto: {header}")
                                try:
                                    conn.sendall(b"ERR_INCOMPLETE_HEADER\n")
                                except Exception:
                                    pass
                                continue

                            serial = header.get("serial", "DESCONOCIDO")
                            self.log_iot(f"🔎 Dispositivo detectado: {serial}")

                            # Ejecutar sesión remota en un hilo (import dinámico tolerante)
                            try:
                                try:
                                    from src.pstrace_connection import ejecutar_sesion_remota_iot
                                except Exception:
                                    mod = importlib.import_module("pstrace_connection")
                                    ejecutar_sesion_remota_iot = getattr(mod, "ejecutar_sesion_remota_iot")

                                method_params = {}  # personaliza según tu sensor
                                threading.Thread(
                                    target=ejecutar_sesion_remota_iot,
                                    args=(serial, method_params, None),
                                    daemon=True
                                ).start()
                                self.log_iot(f"🔧 Sesión remota lanzada para {serial}")
                            except Exception as e:
                                self.log_iot(f"❌ Error ejecutando sesión remota para {serial}: {e}")

                            # Recibir archivo y verificar checksum
                            try:
                                filename = header["filename"]
                                size = int(header["size"])
                                checksum = header["checksum"]

                                filepath = os.path.join(dest_dir, filename)
                                conn.sendall(b"ACK")
                                with open(filepath, "wb") as f:
                                    total_received = 0
                                    while total_received < size:
                                        chunk = conn.recv(buffer_size)
                                        if not chunk:
                                            break
                                        f.write(chunk)
                                        total_received += len(chunk)

                                self.log_iot(f"✅ Archivo recibido: {filename} ({total_received/1e6:.2f} MB)")

                                # Verificar checksum (no fallar el servidor si algo sale mal)
                                try:
                                    actual = hashlib.sha256(open(filepath, "rb").read()).hexdigest()
                                    if actual != checksum:
                                        self.log_iot(f"⚠️ Checksum no coincide: esperado={checksum} actual={actual}")
                                        try:
                                            conn.sendall(b"ERR_CHECKSUM\n")
                                        except Exception:
                                            pass
                                    else:
                                        try:
                                            conn.sendall(b"EOF_OK")
                                        except Exception:
                                            pass
                                except Exception as ex:
                                    self.log_iot(f"⚠️ No se pudo verificar checksum: {ex}")
                                    try:
                                        conn.sendall(b"EOF_OK")
                                    except Exception:
                                        pass

                            except Exception as e:
                                self.log_iot(f"❌ Error en transferencia de archivo: {e}")
                                try:
                                    conn.sendall(b"ERR_TRANSFER\n")
                                except Exception:
                                    pass

                    except socket.timeout:
                        continue
                    except Exception as e:
                        self.log_iot(f"❌ Error en servidor: {e}")
                        continue

            self.server_running = False
            self.log_iot("🛑 Servidor IoT detenido.")

        # Lanzar el hilo
        self.server_thread = threading.Thread(target=server_loop, daemon=True)
        self.server_thread.start()

    def stop_iot_server(self):
        """Detiene el servidor IoT embebido."""
        if not self.server_running:
            self.log_iot("⚠️ El servidor no está activo.")
            return
        self.server_running = False
        self.log_iot("🛑 Solicitando apagado del servidor...")

    def test_iot_connection(self):
        """Prueba la conexión TCP simple con el servidor IoT (envía ping JSON)."""
        host = self.iot_ip_var.get()
        port = self.iot_port_var.get()
        try:
            with socket.create_connection((host, port), timeout=3) as s:
                header = json.dumps({"action": "ping"}).encode() + b"\n"
                s.sendall(header)
                # esperar respuesta corta
                resp = s.recv(128)
                self.log_iot(f"✅ Conectado con {host}:{port} - respuesta: {resp!r}")
                messagebox.showinfo("Conexión exitosa", f"Conectado con {host}:{port}\nRespuesta: {resp.decode(errors='ignore')}")
        except Exception as e:
            self.log_iot(f"❌ Error al conectar: {e}")
            messagebox.showerror("Error de conexión", str(e))

    def send_iot_file(self):
        """Permite seleccionar un archivo y enviarlo al servidor IoT."""
        filepath = filedialog.askopenfilename(title="Seleccionar archivo para enviar")
        if not filepath:
            return

        host = self.iot_ip_var.get()
        port = self.iot_port_var.get()
        size = os.path.getsize(filepath)
        filename = os.path.basename(filepath)

        import hashlib
        checksum = hashlib.sha256(open(filepath, "rb").read()).hexdigest()

        header = json.dumps({
            "action": "send_file",
            "filename": filename,
            "size": size,
            "checksum": checksum,
            # opcional: "serial": "MI_SERIAL"
        }).encode() + b"\n"

        try:
            with socket.create_connection((host, port)) as s:
                s.sendall(header)
                ack = s.recv(8)
                if ack != b"ACK":
                    raise Exception(f"Servidor no aceptó la transferencia (ack={ack!r})")

                self.iot_progress["value"] = 0
                self.iot_progress["maximum"] = size
                self.log_iot(f"📤 Enviando {filename} ({size/1e6:.2f} MB) a {host}:{port}")

                with open(filepath, "rb") as f:
                    sent = 0
                    for chunk in iter(lambda: f.read(4096), b""):
                        s.sendall(chunk)
                        sent += len(chunk)
                        self.iot_progress["value"] = sent
                        self.update_idletasks()

                self.log_iot("✅ Transferencia completada.")
                try:
                    s.sendall(b"EOF")
                except Exception:
                    pass
                messagebox.showinfo("Éxito", f"Archivo {filename} enviado correctamente.")
        except Exception as e:
            self.log_iot(f"❌ Error de envío: {e}")
            messagebox.showerror("Error", str(e))

    # ————— Bloque: Registrar mensajes en ventana de logs —————
    def log_message(self, msg):
        """
        Registra mensajes en la ventana de log (área de texto) y a la vez en el logger.

        Args:
            msg (str): Mensaje a registrar.
        """
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        log.info(msg)



    # ————— Bloque: Exportación de figuras y tablas —————
    def export_figure(self, fig):
        """
        Exporta la figura gráfica actual a un archivo PNG.

        Args:
            fig (matplotlib.figure.Figure): Figura a exportar.
        """
        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG", "*.png")]
        )
        if path:
            fig.savefig(path)
            messagebox.showinfo("Exportar", f"Guardado en {path}")


    def export_ppm(self):
        """
        Exporta la tabla de estimaciones ppm a un archivo CSV.
        """
        if self.ppm_df is None:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")]
        )
        if path:
            self.ppm_df.to_csv(path, index=False)
            messagebox.showinfo("Exportar", f"Guardado en {path}")


    # ————— Bloque: Mostrar estimación ppm en tabla —————
    def show_ppm(self):
        """
        Muestra las estimaciones de ppm en un Treeview:
        - Extrae los nombres de metales de limits_ppm.json
        - Construye un DataFrame con nombres reales de metales
        - Marca alertas usando umbrales específicos para cada metal
        """
        print("[DEBUG] show_ppm() invoked")
        if self.current_data is None:
            print("[DEBUG] show_ppm: sin datos")
            return

        # Cargar límites desde JSON la primera vez
        if not hasattr(self, "limites_ppm"):
            try:
                with open("limits_ppm.json", "r") as f:
                    self.limites_ppm = json.load(f)
            except Exception as e:
                print(f"[ERROR] Error cargando límites: {e}")
                return  # No es posible continuar sin límites

        # Asegurarse de que existen estimaciones en memoria
        if "ppm_estimations" not in self.current_data.columns:
            print("[DEBUG] show_ppm: columna 'ppm_estimations' no existe en DataFrame")
            return

        # Construir dataframe con nombres de metales
        metales = list(self.limites_ppm.keys())
        df = (
            pd.DataFrame(
                self.current_data["ppm_estimations"].tolist(),
                columns=metales
            )
            .fillna(0)
        )
        self.ppm_df = df

        # Configurar columnas del Treeview
        self.tree_ppm.config(columns=metales)
        for metal in metales:
            self.tree_ppm.heading(metal, text=metal)
        self.tree_ppm.delete(*self.tree_ppm.get_children())

        # Poblar filas y resaltar alertas
        for _, row in df.iterrows():
            alerta = any(
                row[metal] > self.limites_ppm.get(metal, float("inf"))
                for metal in metales
            )
            tag = "alert" if alerta else ""
            self.tree_ppm.insert("", "end", values=list(row), tags=(tag,))

        self.tree_ppm.tag_configure("alert", background="#581845", foreground="white")
        ToolTip(
            self.tree_ppm,
            "Tabla de estimaciones ppm; en rojo los valores que exceden el límite específico"
        )

    # ————— Bloque: Botón “Mostrar Clasificación” —————
    def show_classification(self):
        """
        Invocado por el botón "Mostrar Clasificación".
        Refresca la tabla de ppm usando show_ppm().
        """
        print("[DEBUG] show_classification() invoked")
        self.show_ppm()
    
        # ————— Bloque: Exportar clasificación (CSV) —————
    def export_classification(self):
        """
        Exporta la tabla de clasificación (ppm) a un archivo CSV.
        """
        if self.ppm_df is None:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")]
        )
        if path:
            self.ppm_df.to_csv(path, index=False)
            messagebox.showinfo("Exportar", f"Guardado en {path}")



    # ————— Bloque: Cargar archivo y guardar en BD (corregido) —————
    def load_file(self):
        """
        Abre un diálogo para seleccionar un archivo .pssession.
        Procesa la sesión internamente, guarda en BD y actualiza la interfaz.
        """
        print("[DEBUG] load_file() invoked")
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if not path:
            print("[DEBUG] Carga de archivo cancelada por el usuario")
            return
        try:
            print(f"[DEBUG] Procesando archivo: {path}")

            # 1) Importación corregida
            from pstrace_session import extract_session_dict, cargar_limites_ppm as cargar_limites
            print("[DEBUG] Módulo pstrace_session importado correctamente")

            limites = cargar_limites()
            print(f"[DEBUG] Límites PPM cargados: {list(limites.keys()) if limites else 'No disponibles'}")

            data = extract_session_dict(path)
            if not data:
                raise ValueError("No se extrajeron datos de la sesión")
            print("[DEBUG] Datos de sesión extraídos correctamente")

            # 2) Guardar en la base de datos
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            fname = os.path.basename(path)
            now = datetime.datetime.now()

            # Insertar session
            cur.execute(
                """
                INSERT INTO sessions
                  (filename, loaded_at, scan_rate, start_potential,
                   end_potential, software_version)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    fname,
                    now,
                    data["session_info"].get("scan_rate"),
                    data["session_info"].get("start_potential"),
                    data["session_info"].get("end_potential"),
                    data["session_info"].get("software_version"),
                ),
            )
            sid = cur.fetchone()[0]
            print(f"[DEBUG] Sesión insertada en BD. ID: {sid}")

            # Insertar mediciones usando la clave correcta (pca_data o pca_scores)
            for idx, m in enumerate(data["measurements"]):
                # Identificar si la clave existe
                pca_key = "pca_scores" if "pca_scores" in m else "pca_data"
                cur.execute(
                    """
                    INSERT INTO measurements
                      (session_id, title, timestamp, device_serial, curve_count, pca_scores)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sid,
                        m.get("title"),
                        m.get("timestamp"),
                        m.get("device_serial"),
                        m.get("curve_count"),
                        m.get(pca_key),
                    ),
                )
                print(f"[DEBUG] Medición {idx+1} insertada")

            conn.commit()
            conn.close()
            print("[DEBUG] Datos guardados en BD")

            # 3) Actualizar UI
            self.current_data = pd.DataFrame(data["measurements"])
            self.session_info = data["session_info"]
            self.session_info["session_id"] = sid

            self.log_message(f"Sesión {sid} cargada")
            self.txt_detail.delete("1.0", "end")
            self.txt_detail.insert("end", json.dumps(self.session_info, indent=2, ensure_ascii=False))

            # Refrescar selector de curvas
            indices = list(self.current_data.index)
            self.cmb_curve["values"] = indices
            if indices:
                self.cmb_curve.set(indices[0])

            # Mostrar vistas actualizadas
            self.show_curve()
            self.show_pca()
            self.show_ppm()
            self.load_sessions()
            print("[DEBUG] UI actualizada")

        except Exception as e:
            print(f"[ERROR] Error en load_file: {str(e)}")
            import traceback
            traceback.print_exc()
            self.log_message(f"Error carga archivo: {e}")



    # ————— Bloque: (Segunda) Consultar sesiones —————
    def query_sessions_alternative(self):
        """
        Realiza una consulta alternativa de sesiones basado en el ID y fecha.
        Actualiza la tabla de resultados con los datos obtenidos.
        """
        sid_text = self.id_entry.get().strip()
        date_text = self.date_start.get_date().strftime("%Y-%m-%d")
        where_clauses = []
        params = []

        if sid_text:
            where_clauses.append("s.id = %s")
            params.append(int(sid_text))
        if date_text:
            where_clauses.append("s.loaded_at::date = %s")
            params.append(date_text)

        where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""
        sql = f"""
            SELECT s.id, s.filename, s.loaded_at AS Fecha,
                   m.device_serial AS Sensor,
                   m.curve_count AS CurveCount,
                   CASE WHEN MAX(p.current) > %s THEN '⚠️ Contaminación' ELSE '✅ Limpio' END AS Estado
            FROM sessions s
            JOIN measurements m ON s.id = m.session_id
            LEFT JOIN curves c ON m.id = c.measurement_id
            LEFT JOIN points p ON c.id = p.curve_id
            WHERE 1=1
            {where_sql}
            GROUP BY s.id, s.filename, s.loaded_at, m.device_serial, m.curve_count
        """
        params.insert(0, self.settings["alert_threshold"])

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            conn.close()

            self.tree.delete(*self.tree.get_children())
            for row in rows:
                self.tree.insert("", "end", values=row)
        except Exception as e:
            self.log_message(f"Error en consulta: {e}")

    # ————— Bloque: Mostrar curvas individuales y promedio —————
    def show_curve(self):
        """
        Dibuja la(s) curva(s) de voltametría para la medición seleccionada.
        """
        print("[DEBUG] show_curve() invoked")
        if self.current_data is None or self.cmb_curve.get() == "":
            print("[DEBUG] show_curve: sin datos o índice vacío")
            return

        # Índice de medición seleccionado
        idx = int(self.cmb_curve.get())
        arrs = self.current_data.at[idx, "pca_scores"] or []
        n = len(arrs) // len(self.settings["cycles"]) if arrs else 1
        curvas = [arrs[i * n : (i + 1) * n] for i in range(len(self.settings["cycles"]))]
        x = list(range(n))

        # Limpiar ejes
        self.ax_curve.clear()

        # Graficar curvas individuales
        for curve in curvas:
            self.ax_curve.plot(x, curve, alpha=0.3, linewidth=1)

        # Promedio y desviación estándar
        df = pd.DataFrame(curvas).T
        mean = df.mean(axis=1)
        std = df.std(axis=1)
        self.ax_curve.plot(x, mean, color="#e74c3c", linewidth=2, label="Promedio")
        self.ax_curve.fill_between(x, mean - std, mean + std, color="#e74c3c", alpha=0.2)

        # Título y etiquetas
        si = self.session_info
        sensor = self.current_data.at[idx, "device_serial"] or "N/A"
        self.ax_curve.set_title(f"Sesión {si['session_id']} · Sensor {sensor}", color="white")
        self.ax_curve.set_xlabel("Índice de punto", color="white")
        self.ax_curve.set_ylabel("Corriente (A)", color="white")

        # Leyenda y cuadrícula
        self.ax_curve.legend(facecolor=COLOR_BG, labelcolor="white")
        self.ax_curve.grid(True, color="#5d6d7e")

        # Redibujar canvas
        self.canvas_curve.draw()
        ToolTip(self.canvas_curve.get_tk_widget(), "Aquí ves la(s) curva(s) y su promedio con desviación estándar")

    # ————— Bloque: Mostrar PCA y varianza —————
    def show_pca(self):
        """
        Calcula y muestra el PCA de los vectores pca_scores de todas las mediciones.
        """
        print("[DEBUG] show_pca() invoked")
        if self.current_data is None or "pca_scores" not in self.current_data:
            print("[DEBUG] show_pca: sin datos")
            return

        # Matriz de datos
        df = pd.DataFrame(self.current_data["pca_scores"].tolist()).fillna(0)

        # Ajuste PCA
        pca = PCA().fit(df)
        var = pca.explained_variance_ratio_.cumsum() * 100

        # Limpiar ejes
        self.ax_pca.clear()

        # Graficar varianza acumulada
        self.ax_pca.plot(range(1, len(var) + 1), var, marker="o", linewidth=2)
        for i, v in enumerate(var[:3], start=1):
            self.ax_pca.annotate(
                f"{v:.1f}%",
                (i, v),
                textcoords="offset points",
                xytext=(0, 5),
                ha="center",
                color="white"
            )

        # Estética
        self.ax_pca.set_ylim(0, max(110, max(var) + 5))

        self.ax_pca.set_title("Varianza Acumulada PCA", color="white")
        self.ax_pca.set_xlabel("Componentes", color="white")
        self.ax_pca.set_ylabel("Varianza (%)", color="white")
        self.ax_pca.grid(True, color="#5d6d7e")

        # Redibujar canvas
        self.canvas_pca.draw()
        ToolTip(self.canvas_pca.get_tk_widget(), "Aquí ves la varianza acumulada de cada componente del PCA")

    # ——— Bloque: Pestaña “PCA” ———
    def build_pca_tab(self, parent):
        """
        Configura la pestaña 'PCA' para visualizar el análisis de componentes principales.

        Args:
            parent (ttk.Notebook): Notebook donde se añade la pestaña.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="📈 PCA")

        btn_show_pca = ttk.Button(f, text="Mostrar PCA", command=self.show_pca)
        btn_show_pca.pack(pady=8)
        ToolTip(btn_show_pca, "Calcula y muestra la gráfica de varianza acumulada del PCA")

        self.fig_pca, self.ax_pca = plt.subplots(figsize=(9, 5), facecolor=COLOR_BG)
        self.ax_pca.set_facecolor(COLOR_BG)
        self.ax_pca.tick_params(colors="white")
        self.ax_pca.grid(True, color="#5d6d7e")
        self.canvas_pca = FigureCanvasTkAgg(self.fig_pca, master=f)
        self.canvas_pca.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        ToolTip(self.canvas_pca.get_tk_widget(), "Gráfica de varianza acumulada resultante del PCA")

        btn_export_pca = ttk.Button(
            f,
            text="Exportar PCA",
            command=lambda: self.export_figure(self.fig_pca)
        )
        btn_export_pca.pack(side="right", padx=10, pady=5)
        ToolTip(btn_export_pca, "Exporta la gráfica de PCA como imagen PNG")



    # ————— Bloque: Cargar lista de sesiones (para futuras funciones) —————
    def load_sessions(self):
        """
        Carga en memoria la lista de IDs de sesiones registradas en la base de datos.
        """
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT id FROM sessions")
            _ = [r[0] for r in cur.fetchall()]
            conn.close()
        except Exception as e:
            self.log_message(f"Error cargando sesiones: {e}")

    # ————— Bloque: Ventana de ajustes (segunda definición) —————
    def show_settings_alternative(self):
        """
        Abre una ventana para editar los ciclos a promediar en base a la configuración actual.
        Guarda los cambios mediante la función save_settings().
        """
        w = tk.Toplevel(self)
        w.title("Ajustes")
        ttk.Label(w, text="Ciclos a promediar:").pack(pady=5)
        e = ttk.Entry(w)
        e.insert(0, ",".join(map(str, self.settings["cycles"])))
        e.pack(pady=5)

        def save():
            self.settings["cycles"] = [int(x) for x in e.get().split(",")]
            self.save_settings()
            w.destroy()

        ttk.Button(w, text="Guardar", command=save).pack(pady=10)

if __name__ == "__main__":
    app = Aplicacion()
    app.mainloop()