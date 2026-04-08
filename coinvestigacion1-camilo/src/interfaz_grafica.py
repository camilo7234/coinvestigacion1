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
                if self.widget.winfo_class() in ("Text", "Entry"):
                    x, y, cx, cy = self.widget.bbox("insert")
                else:
                    x, y, cx, cy = (0, 0, 0, 0)
            else:
                x, y, cx, cy = (0, 0, 0, 0)
        except Exception:
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
DB_CONFIG = {
    "host": "ep-lucky-morning-adafnn5y-pooler.c-2.us-east-1.aws.neon.tech",
    "user": "neondb_owner",
    "password": "npg_pgxVl1e3BMqH",
    "database": "neondb",
    "port": 5432
}

COLOR_BG = "#2c3e50"
COLOR_BT = "#3498db"
COLOR_FG = "#ffffff"
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

        self.current_data = None
        self.session_info = {}
        self.ppm_df = None
        self.settings = self.load_settings()

        self.setup_style()
        self.create_menu()
        self.create_tabs()

        # 🟢 Llamada diferida: espera a que la GUI esté lista antes de cargar sesiones
        self.after(800, lambda: self.load_sessions())

    def load_devices(self):
        """
        Consulta la base de datos para cargar los seriales de dispositivos
        y asignarlos al combobox, siempre incluyendo la opción "— Todos —".
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

            options = ["— Todos —"] + vals if vals else ["— Todos —"]
            print(f"[DEBUG] load_devices: valores obtenidos: {options}")
            self.device_combobox["values"] = options
            self.device_combobox.current(0)

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

    def load_file(self):
        """
        Llama al método de selección de archivo para cargar .pssession.
        """
        return self.seleccionar_archivo()

    def load_settings(self):
        """
        Carga los ajustes del archivo settings.json.
        Si no existe o hay error, utiliza los ajustes por defecto.
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
        Cargar Datos, Consultas, Detalle, Curvas, PCA, ppm e IoT.
        Se usa llamada diferida (after) para evitar errores de inicialización
        cuando Tk aún no ha vinculado completamente la instancia.
        """
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)

        # Pestañas principales inmediatas
        self.build_load_tab(nb)
        self.build_query_tab(nb)

        # ⚙️ Pestañas diferidas (evita AttributeError en arranque)
        self.after(200, lambda: self.build_detail_tab(nb))
        self.after(300, lambda: self.build_curve_tab(nb))
        self.after(400, lambda: self.build_pca_tab(nb))
        self.after(500, lambda: self.build_ppm_tab(nb))
        self.after(600, lambda: self.build_iot_tab(nb))

    # ————— Bloque: Pestaña "Cargar Datos" —————
    def build_load_tab(self, parent):
        """
        Configura la pestaña de carga de datos (.pssession).
        """
        f = ttk.Frame(parent)
        parent.add(f, text="📤 Cargar Datos")
        ttk.Button(f, text="Seleccionar Archivo .pssession", command=self.load_file).pack(pady=20)
        self.log_text = tk.Text(f, height=8, bg="#34495e", fg="white", font=("Courier", 10))
        self.log_text.pack(fill="x", padx=10, pady=10)

    # ——— Bloque 2.1 ———
    def show_settings_alternative(self):
        """
        Ventana de Configuraciones Alternativas.
        Permite al usuario ajustar el umbral de alerta (ppm).
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
        Armado de la pestaña 'Consultas'.
        """
        print("[DEBUG] build_query_tab() invoked")

        tab = ttk.Frame(parent)
        parent.add(tab, text="🔍 Consultas")

        self._create_overview_panel(tab)
        self._create_filters_panel(tab)
        self._create_results_table(tab)
        self._create_meta_panel(tab)

        self.lbl_status = ttk.Label(
            tab,
            text="⏳ Cargando información...",
            font=("Segoe UI", 10, "italic"),
            foreground="#555"
        )
        self.lbl_status.pack(pady=4)

        self.after(100, lambda: self._safe_exec(self.load_devices, "load_devices"))
        self.after(200, lambda: self._safe_exec(self.set_default_date_range, "set_default_date_range"))
        self.after(400, lambda: self._safe_exec(self.update_overview, "update_overview"))
        self.after(600, lambda: self._safe_exec(self.query_sessions, "query_sessions"))
        self.after(1000, lambda: self._finalize_build_query_tab())

    def _safe_exec(self, func, name):
        """Ejecución segura con logging y actualización visual."""
        try:
            print(f"[DEBUG] Ejecutando {name}()")
            func()
            if hasattr(self, "lbl_status"):
                self.lbl_status.config(text=f"✅ {name} completado")
        except Exception as e:
            print(f"[ERROR] Fallo en {name}(): {e}")
            if hasattr(self, "lbl_status"):
                self.lbl_status.config(text=f"⚠️ Error en {name}")
            import traceback
            traceback.print_exc()

    def _finalize_build_query_tab(self):
        """Actualiza el estado final de la pestaña una vez cargada."""
        if hasattr(self, "lbl_status"):
            self.lbl_status.config(
                text=f"🟢 Consultas listas — {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
                foreground="#1b5e20"
            )
        print("[DEBUG] build_query_tab completado correctamente.")

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

        ttk.Label(container, text="ID Sesión:").grid(row=0, column=0, sticky="e", padx=5)
        self.id_entry = ttk.Entry(container, width=10)
        self.id_entry.grid(row=0, column=1, padx=5)
        ToolTip(self.id_entry, "Introduce el ID de la sesión para filtrar los resultados")

        ttk.Label(container, text="Fecha Inicio:").grid(row=0, column=2, sticky="e", padx=5)
        self.date_start = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_start.grid(row=0, column=3, padx=5)
        ToolTip(self.date_start, "Selecciona la fecha de inicio del rango de búsqueda")

        ttk.Label(container, text="Fecha Fin:").grid(row=0, column=4, sticky="e", padx=5)
        self.date_end = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_end.grid(row=0, column=5, padx=5)
        ToolTip(self.date_end, "Selecciona la fecha de fin del rango de búsqueda")

        ttk.Label(container, text="Dispositivo:").grid(row=1, column=0, sticky="e", padx=5, pady=(10, 0))
        self.device_combobox = ttk.Combobox(container, state="readonly", width=12)
        self.device_combobox.grid(row=1, column=1, padx=5, pady=(10, 0))
        self.device_combobox["values"] = ["— Todos —"]
        self.device_combobox.current(0)
        ToolTip(self.device_combobox, "Filtra los resultados por el número de serie del dispositivo")

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

    # ——— Bloque 2.2.3 ———
    def _create_results_table(self, parent):
        """
        Crea la tabla de resultados con la columna 'Contaminantes'
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

    # ——— Bloque 2.2.4 ———
    def _create_meta_panel(self, parent):
        """
        Panel de detalles técnicos de la sesión seleccionada.
        """
        print("[DEBUG] _create_meta_panel() invoked")
        frame = ttk.LabelFrame(parent, text="Detalles Técnicos de la Sesión Seleccionada")
        frame.pack(fill="x", padx=10, pady=5)
        meta_content = ttk.Frame(frame)
        meta_content.pack(fill="x", padx=10, pady=10)

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

    # ——— Bloque 2.3 ———
    def query_sessions(self):
        """
        Consulta sesiones y llena la tabla con valores numéricos bien formateados.
        """
        log.debug("query_sessions() (unificado) invoked")

        sid_text = getattr(self, "id_entry", tk.Entry()).get().strip() if hasattr(self, "id_entry") else ""
        try:
            session_id = int(sid_text) if sid_text else None
        except Exception:
            log.debug(f"ID inválido: '{sid_text}' – ignorando filtro de ID.")
            session_id = None

        try:
            start_date = self.date_start.get_date().strftime("%Y-%m-%d")
            end_date = self.date_end.get_date().strftime("%Y-%m-%d")
        except Exception:
            today = datetime.date.today()
            start_date = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")

        device = getattr(self, "device_combobox", None)
        device_val = device.get() if device else None
        use_device_filter = bool(device_val and device_val != "— Todos —")

        alert_threshold = float(self.settings.get("alert_threshold", 0.5))
        anom_threshold = float(self.settings.get("anom_threshold", alert_threshold * 0.5))

        sql = """
            SELECT
            s.id,
            s.filename,
            s.loaded_at::date AS fecha,
            m.device_serial AS dispositivo,
            m.curve_count AS curvas,
            COALESCE(m.classification_group, 0) AS classification_group,
            COALESCE(m.contamination_level::numeric, 0) AS contamination_level,
            COALESCE(m.title, '') AS contaminantes
            FROM sessions s
            JOIN measurements m ON s.id = m.session_id
            WHERE s.loaded_at::date BETWEEN %s AND %s
        """
        params = [start_date, end_date]
        if session_id is not None:
            sql += "  AND s.id = %s\n"
            params.append(session_id)
        if use_device_filter:
            sql += "  AND m.device_serial = %s\n"
            params.append(device_val)

        sql += " ORDER BY s.loaded_at DESC"

        log.debug("SQL a ejecutar en query_sessions (unificado):\n%s", sql)
        log.debug("Params: %s", params)

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("Error en query_sessions (DB): %s", e)
            messagebox.showerror("Error en consulta", f"No se pudo ejecutar la consulta:\n{e}")
            return

        if not hasattr(self, "tree"):
            log.debug("query_sessions: no existe self.tree, abortando llenado UI.")
            return

        self.tree.delete(*self.tree.get_children())

        if not rows:
            self.tree.insert("", "end", values=("--", "Sin resultados", "--", "--", "--", "--", "--", "--"))
            return

        inserted = 0

        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
        limits_path = os.path.join(project_root, "limits_ppm.json")

        try:
            with open(limits_path, "r", encoding="utf-8") as fh:
                limits_data = json.load(fh)
        except Exception:
            limits_data = {}
            log.warning("No se pudo cargar limits_ppm.json; usando límites por defecto.")
            limits_data = {"Cd": 0.1, "Zn": 3.0, "Cu": 1.0, "Cr": 0.5, "Ni": 0.5}

        for r in rows:
            sid = r[0]
            filename = r[1] or ""
            fecha = r[2] or ""
            dispositivo = r[3] or ""
            curvas = r[4] if r[4] is not None else ""
            classification_group = int(r[5]) if r[5] is not None else 1.0

            try:
                contamination_level = float(r[6]) if r[6] is not None else 1.0
            except Exception:
                try:
                    contamination_level = float(str(r[6]).replace(",", "."))
                except Exception:
                    contamination_level = 0.0

            contaminantes = r[7] or ""

            estado_str = "✅ 🟡"
            tag = "warning"

            excede_limite = any(
                contamination_level > limite for limite in limits_data.values()
            )
            cercano_limite = any(
                contamination_level > (25 * limite) for limite in limits_data.values()
            )

            if excede_limite:
                estado_str = "⚠️ CONTAMINADA"
                tag = "alert"
            elif cercano_limite:
                estado_str = "🟡 ANÓMALA"
                tag = "warning"

            ppm_display = f"{contamination_level:,.2f}" if contamination_level != 0 else "0.00"

            values = (sid, filename, fecha, dispositivo, curvas, estado_str, ppm_display, contaminantes)
            self.tree.insert("", "end", values=values, tags=(tag,))
            inserted += 1

        self.tree.tag_configure("alert", background="#ffebee", foreground="#c62828")
        self.tree.tag_configure("warning", background="#fff9c4", foreground="#f57f17")
        self.tree.tag_configure("safe", background="#e8f5e9", foreground="#2e7d32")

        log.debug("query_sessions: insertadas %d filas en la tabla (UI).", inserted)

        try:
            self.update_overview()
        except Exception as e:
            log.debug("update_overview fallo desde query_sessions: %s", e)

    # ——— Bloque 2.6 ———
    def update_overview(self):
        """
        Actualiza la vista general mostrando estadísticas globales.
        """
        print("[DEBUG] update_overview() invoked")
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
            for lbl in getattr(self, "overview_labels", {}).values():
                lbl.config(text="--")

    # ——— Bloque 2.7 ———
    def set_default_date_range(self):
        """
        Establece el rango de fechas a los últimos 7 días y dispara la consulta.
        """
        print("[DEBUG] set_default_date_range() invoked")
        if not hasattr(self, "date_start") or not hasattr(self, "date_end"):
            print("[DEBUG] set_default_date_range: no existen date_start/date_end, skip.")
            return

        try:
            today = datetime.date.today()
            last7 = today - datetime.timedelta(days=7)
            self.date_start.set_date(last7)
            self.date_end.set_date(today)
            print(f"[DEBUG] set_default_date_range: date_start={last7}, date_end={today}")
            self.query_sessions()
        except Exception as e:
            print(f"[DEBUG] set_default_date_range Error: {e}")
            messagebox.showerror("Error", f"Error estableciendo rango de fechas:\n{e}")

    # ——————————————————————————————————————————————————
    # ————— Bloque: Pestaña "Detalle Sesión" — CORREGIDO
    # ——————————————————————————————————————————————————
    def build_detail_tab(self, parent):
        """
        Crea la pestaña 'Detalle Sesión' con vista estructurada y visual.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="📝 Detalle Sesión")

        # Encabezado
        header = tk.Frame(f, bg="#1a252f", pady=10)
        header.pack(fill="x", padx=0, pady=0)
        tk.Label(
            header,
            text="📋 Información de la Sesión",
            bg="#1a252f",
            fg="#3498db",
            font=("Arial", 13, "bold"),
        ).pack(padx=15)

        # Área scrolleable
        canvas = tk.Canvas(f, bg=COLOR_BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(f, orient="vertical", command=canvas.yview)
        self.detail_scroll_frame = tk.Frame(canvas, bg=COLOR_BG)

        self.detail_scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.detail_scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True, padx=10, pady=10)

        # Mensaje inicial
        tk.Label(
            self.detail_scroll_frame,
            text="Carga un archivo .pssession o selecciona una sesión para ver los detalles.",
            bg=COLOR_BG,
            fg="#7f8c8d",
            font=("Arial", 10, "italic"),
            wraplength=700,
            justify="left",
        ).pack(padx=20, pady=20)

    def _populate_detail_view(self, info: dict):
        """
        Rellena la pestaña Detalle Sesión con los campos del diccionario info
        de forma estructurada y visualmente agradable.
        No muestra el campo scan_rate (velocidad de escaneo).
        """
        for widget in self.detail_scroll_frame.winfo_children():
            widget.destroy()

        if not info:
            tk.Label(
                self.detail_scroll_frame,
                text="No hay información disponible.",
                bg=COLOR_BG,
                fg="#7f8c8d",
                font=("Arial", 10, "italic"),
            ).pack(padx=20, pady=20)
            return

        # Campos a mostrar con etiquetas amigables (scan_rate excluido)
        LABEL_MAP = {
            "session_id":           "🆔  ID de Sesión",
            "filename":             "📄  Archivo",
            "loaded_at":            "📅  Fecha de Carga",
            "start_potential":      "🔋  Potencial Inicial",
            "end_potential":        "🔋  Potencial Final",
            "software_version":     "💾  Versión de Software",
            "device_serial":        "🔌  Serial del Dispositivo",
            "curve_count":          "📈  Número de Curvas",
            "contamination_level":  "⚗️  Nivel de Contaminación",
            "classification_group": "🗂  Grupo de Clasificación",
        }

        # Campos a omitir
        SKIP_KEYS = {"scan_rate"}

        known_keys = list(LABEL_MAP.keys())
        ordered_keys = [k for k in known_keys if k in info and k not in SKIP_KEYS] + \
                       [k for k in info if k not in known_keys and k not in SKIP_KEYS]

        for i, key in enumerate(ordered_keys):
            value = info[key]
            if isinstance(value, dict):
                val_str = json.dumps(value, ensure_ascii=False, indent=2)
            elif value is None:
                val_str = "—"
            else:
                val_str = str(value)

            label_text = LABEL_MAP.get(key, f"🔹  {key.replace('_', ' ').title()}")

            row_bg = "#34495e" if i % 2 == 0 else "#2c3e50"
            row = tk.Frame(self.detail_scroll_frame, bg=row_bg, pady=6)
            row.pack(fill="x", padx=5, pady=1)

            tk.Label(
                row,
                text=label_text,
                bg=row_bg,
                fg="#3498db",
                font=("Arial", 10, "bold"),
                width=28,
                anchor="w",
            ).pack(side="left", padx=(12, 4))

            if "\n" in val_str:
                txt = tk.Text(
                    row,
                    bg=row_bg,
                    fg="#ecf0f1",
                    font=("Courier", 9),
                    height=min(val_str.count("\n") + 1, 6),
                    width=60,
                    relief="flat",
                    wrap="none",
                )
                txt.insert("1.0", val_str)
                txt.config(state="disabled")
                txt.pack(side="left", padx=4, pady=2)
            else:
                tk.Label(
                    row,
                    text=val_str,
                    bg=row_bg,
                    fg="#ecf0f1",
                    font=("Arial", 10),
                    anchor="w",
                    wraplength=700,
                    justify="left",
                ).pack(side="left", padx=4)

    # ————— Bloque: Pestaña "Curvas" —————
    def build_curve_tab(self, parent):
        """
        Configura la pestaña 'Curvas' para visualizar curvas individuales y promedio.
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

    # ——— Bloque: Pestaña "PCA" — ORIGINAL RESTAURADO ———
    def build_pca_tab(self, parent):
        """
        Configura la pestaña 'PCA' para visualizar el análisis de componentes principales.
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

    # ——————————————————————————————————————————————————
    # ————— Bloque: Pestaña "Clasificación" — CORREGIDO
    # ——————————————————————————————————————————————————
    def build_ppm_tab(self, parent):
        """
        Configura la pestaña 'Clasificación'.
        El Treeview se crea como placeholder; show_ppm lo destruye
        y recrea dinámicamente con las columnas correctas de metales.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="🗂 Clasificación")

        # Guardamos referencia al frame para poder recrear el treeview dentro
        self._ppm_tab_frame = f

        btn_show_ppm = ttk.Button(
            f,
            text="Mostrar Clasificación",
            command=self.show_classification
        )
        btn_show_ppm.pack(pady=8)
        ToolTip(btn_show_ppm, "Dibuja la clasificación y nivel de contaminación")

        # Contenedor del treeview (se reemplaza en show_ppm)
        self._ppm_tree_container = tk.Frame(f, bg=COLOR_BG)
        self._ppm_tree_container.pack(fill="both", expand=True, padx=10, pady=5)

        # Treeview inicial placeholder
        self.tree_ppm = ttk.Treeview(
            self._ppm_tree_container,
            columns=("Metal", "Valor (ppm)", "Límite", "Estado"),
            show="headings",
            height=8,
        )
        for col in ("Metal", "Valor (ppm)", "Límite", "Estado"):
            self.tree_ppm.heading(col, text=col)
            self.tree_ppm.column(col, anchor="center", width=120)
        self.tree_ppm.pack(fill="both", expand=True)
        ToolTip(self.tree_ppm, "Tabla con estimaciones PPM por metal. Carga una sesión y pulsa 'Mostrar Clasificación'.")

        btn_export_ppm = ttk.Button(
            f,
            text="Exportar Clasificación",
            command=self.export_classification
        )
        btn_export_ppm.pack(side="right", padx=10, pady=5)
        ToolTip(btn_export_ppm, "Exporta la tabla de clasificación como CSV")

    # ————— Bloque: Pestaña "IoT / Comunicación" —————
    def build_iot_tab(self, parent):
        """
        Crea la pestaña para control del servidor IoT, conexión remota y envío de archivos.
        """
        f = ttk.Frame(parent)
        parent.add(f, text="🌐 IoT / Comunicación")

        ttk.Label(f, text="Centro de Control IoT", font=("Arial", 14, "bold")).pack(pady=10)

        frame_conf = ttk.LabelFrame(f, text="Configuración del Servidor / Cliente")
        frame_conf.pack(fill="x", padx=10, pady=10)

        ttk.Label(frame_conf, text="IP del servidor remoto:").grid(row=0, column=0, padx=5, sticky="e")
        self.iot_ip_var = tk.StringVar(value="")
        ttk.Entry(frame_conf, textvariable=self.iot_ip_var, width=18).grid(row=0, column=1, padx=5)

        ttk.Label(frame_conf, text="Puerto:").grid(row=0, column=2, padx=5, sticky="e")
        self.iot_port_var = tk.IntVar(value=5000)
        ttk.Entry(frame_conf, textvariable=self.iot_port_var, width=8).grid(row=0, column=3, padx=5)

        frame_srv = ttk.LabelFrame(f, text="Servidor IoT Local")
        frame_srv.pack(fill="x", padx=10, pady=5)

        self.server_running = False
        self.server_thread = None

        ttk.Button(frame_srv, text="🚀 Iniciar Servidor", command=self.start_iot_server).grid(row=0, column=0, padx=5, pady=5)
        ttk.Button(frame_srv, text="🛑 Detener Servidor", command=self.stop_iot_server).grid(row=0, column=1, padx=5, pady=5)

        frame_cli = ttk.LabelFrame(f, text="Cliente IoT (modo remoto)")
        frame_cli.pack(fill="x", padx=10, pady=5)

        ttk.Button(frame_cli, text="🔌 Probar Conexión", command=self.test_iot_connection).grid(row=0, column=0, padx=5)
        ttk.Button(frame_cli, text="📤 Enviar Archivo", command=self.send_iot_file).grid(row=0, column=1, padx=5)

        ttk.Label(f, text="Progreso de envío:").pack(pady=(15, 5))
        self.iot_progress = ttk.Progressbar(f, length=500, mode="determinate")
        self.iot_progress.pack(pady=5)

        self.iot_log = tk.Text(f, height=12, bg="#1e272e", fg="white", font=("Courier", 10))
        self.iot_log.pack(fill="x", padx=10, pady=10)
        ToolTip(self.iot_log, "Registro detallado de eventos IoT")

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
            import sys
            import importlib
            import threading
            import hashlib
            import socket
            import json
            import os

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

                            if header_text.lower() == "ping" or header_text.lower() == "ping\n":
                                self.log_iot(f"📡 Ping recibido (texto) desde {addr}")
                                try:
                                    conn.sendall(b"PONG\n")
                                except Exception:
                                    pass
                                continue

                            try:
                                header = json.loads(header_text)
                            except Exception as e:
                                self.log_iot(f"❌ Encabezado inválido (no JSON): {header_text!r} - {e}")
                                try:
                                    conn.sendall(b"ERR_INVALID_HEADER\n")
                                except Exception:
                                    pass
                                continue

                            if isinstance(header, dict) and header.get("action") == "ping":
                                self.log_iot(f"📡 Ping JSON recibido desde {addr}")
                                try:
                                    conn.sendall(b"PONG\n")
                                except Exception:
                                    pass
                                continue

                            if not all(k in header for k in ("filename", "size", "checksum")):
                                self.log_iot(f"❌ Encabezado incompleto: {header}")
                                try:
                                    conn.sendall(b"ERR_INCOMPLETE_HEADER\n")
                                except Exception:
                                    pass
                                continue

                            serial = header.get("serial", "DESCONOCIDO")
                            self.log_iot(f"🔎 Dispositivo detectado: {serial}")

                            try:
                                try:
                                    from src.pstrace_connection import ejecutar_sesion_remota_iot
                                except Exception:
                                    mod = importlib.import_module("pstrace_connection")
                                    ejecutar_sesion_remota_iot = getattr(mod, "ejecutar_sesion_remota_iot")

                                method_params = {}
                                threading.Thread(
                                    target=ejecutar_sesion_remota_iot,
                                    args=(serial, method_params, None),
                                    daemon=True
                                ).start()
                                self.log_iot(f"🔧 Sesión remota lanzada para {serial}")
                            except Exception as e:
                                self.log_iot(f"❌ Error ejecutando sesión remota para {serial}: {e}")

                            try:
                                fname = header["filename"]
                                fsize = header["size"]
                                expected_checksum = header["checksum"]
                                dest_path = os.path.join(dest_dir, fname)

                                conn.sendall(b"READY\n")
                                received = b""
                                while len(received) < fsize:
                                    chunk = conn.recv(min(buffer_size, fsize - len(received)))
                                    if not chunk:
                                        break
                                    received += chunk
                                    progress = int(len(received) / fsize * 100)
                                    self.after(0, lambda p=progress: self.iot_progress.configure(value=p))

                                actual_checksum = hashlib.md5(received).hexdigest()
                                if actual_checksum == expected_checksum:
                                    with open(dest_path, "wb") as out:
                                        out.write(received)
                                    conn.sendall(b"OK\n")
                                    self.log_iot(f"✅ Archivo recibido: {fname}")
                                    self.after(0, lambda p=dest_path: self.process_received_file(p))
                                else:
                                    conn.sendall(b"ERR_CHECKSUM\n")
                                    self.log_iot(f"❌ Checksum inválido para {fname}")

                            except Exception as e:
                                self.log_iot(f"❌ Error recibiendo archivo: {e}")

                    except socket.timeout:
                        continue
                    except Exception as e:
                        if self.server_running:
                            self.log_iot(f"❌ Error en servidor: {e}")

            self.log_iot("🔴 Servidor detenido.")

        self.server_thread = threading.Thread(target=server_loop, daemon=True)
        self.server_thread.start()

    def stop_iot_server(self):
        """Detiene el servidor IoT."""
        self.server_running = False
        self.log_iot("🛑 Señal de detención enviada al servidor.")

    def test_iot_connection(self):
        """Prueba la conexión con el servidor IoT remoto."""
        ip = self.iot_ip_var.get().strip()
        port = self.iot_port_var.get()
        if not ip:
            messagebox.showwarning("Advertencia", "Introduce una IP válida.")
            return

        def do_test():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((ip, port))
                    ping_msg = json.dumps({"action": "ping"}) + "\n"
                    s.sendall(ping_msg.encode())
                    resp = s.recv(64).decode(errors="replace").strip()
                    if resp == "PONG":
                        self.after(0, lambda: self.log_iot(f"✅ Conexión exitosa con {ip}:{port}"))
                    else:
                        self.after(0, lambda: self.log_iot(f"⚠️ Respuesta inesperada: {resp}"))
            except Exception as e:
                self.after(0, lambda: self.log_iot(f"❌ Fallo de conexión: {e}"))

        threading.Thread(target=do_test, daemon=True).start()

    def send_iot_file(self):
        """Envía un archivo .pssession al servidor IoT remoto."""
        ip = self.iot_ip_var.get().strip()
        port = self.iot_port_var.get()
        if not ip:
            messagebox.showwarning("Advertencia", "Introduce una IP válida.")
            return

        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession"), ("Todos", "*.*")])
        if not path:
            return

        def do_send():
            import hashlib
            try:
                with open(path, "rb") as fh:
                    data = fh.read()
                checksum = hashlib.md5(data).hexdigest()
                fname = os.path.basename(path)
                header = json.dumps({
                    "filename": fname,
                    "size": len(data),
                    "checksum": checksum
                }) + "\n"

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(30)
                    s.connect((ip, port))
                    s.sendall(header.encode())
                    resp = s.recv(64).decode(errors="replace").strip()
                    if resp != "READY":
                        self.after(0, lambda: self.log_iot(f"❌ Servidor no está listo: {resp}"))
                        return

                    chunk_size = 4096
                    sent = 0
                    while sent < len(data):
                        chunk = data[sent:sent + chunk_size]
                        s.sendall(chunk)
                        sent += len(chunk)
                        progress = int(sent / len(data) * 100)
                        self.after(0, lambda p=progress: self.iot_progress.configure(value=p))

                    final_resp = s.recv(64).decode(errors="replace").strip()
                    if final_resp == "OK":
                        self.after(0, lambda: self.log_iot(f"✅ Archivo enviado correctamente: {fname}"))
                    else:
                        self.after(0, lambda: self.log_iot(f"❌ Error en envío: {final_resp}"))

            except Exception as e:
                self.after(0, lambda: self.log_iot(f"❌ Error enviando archivo: {e}"))

        threading.Thread(target=do_send, daemon=True).start()

    def process_received_file(self, path):
        """Procesa un archivo recibido por IoT."""
        self.log_iot(f"🔄 Procesando archivo recibido: {os.path.basename(path)}")
        try:
            from pstrace_session import extract_session_dict
            data = extract_session_dict(path)
            if data:
                self.current_data = pd.DataFrame(data["measurements"])
                self.session_info = data["session_info"]
                self.log_iot(f"✅ Sesión procesada exitosamente")
                self.show_curve()
                self.show_pca()
                self.show_ppm()
        except Exception as e:
            self.log_iot(f"❌ Error procesando archivo: {e}")

    def log_message(self, msg):
        """
        Registra mensajes en la ventana de log (área de texto) y a la vez en el logger.
        """
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        log.info(msg)

    # ————— Bloque: Exportación de figuras y tablas —————
    def export_figure(self, fig):
        """
        Exporta la figura gráfica actual a un archivo PNG.
        """
        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG", "*.png")]
        )
        if path:
            fig.savefig(path)
            messagebox.showinfo("Exportar", f"Guardado en {path}")

    # ————— Bloque: Mostrar estimación ppm en tabla —————
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

    # ——————————————————————————————————————————————————
    # ————— show_ppm — CORREGIDO (Treeview dinámico)
    # ——————————————————————————————————————————————————
    def show_ppm(self):
        """
        Muestra las estimaciones de ppm en un Treeview.
        Destruye y recrea el Treeview con las columnas correctas de metales
        para evitar el bug de Tkinter con .config(columns=...).
        """
        print("[DEBUG] show_ppm() invoked")

        if self.current_data is None or self.current_data.empty:
            print("[DEBUG] show_ppm: sin datos de sesión")
            return

        # Cargar límites desde limits_ppm.json
        # ✅ DESPUÉS (correcto — sube un nivel a la raíz del proyecto)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
        limits_path = os.path.join(project_root, "limits_ppm.json")
        try:
            with open(limits_path, "r", encoding="utf-8") as f:
                self.limites_ppm = json.load(f)
        except Exception as e:
            print(f"[ERROR] No se pudo cargar limits_ppm.json: {e}")
            self.limites_ppm = {"Cd": 0.1, "Zn": 3.0, "Cu": 1.0, "Cr": 0.5, "Ni": 0.5}
        metales = list(self.limites_ppm.keys())
        if not metales:
            print("[DEBUG] No hay metales definidos en limits_ppm.json")
            return

        if "ppm_estimations" not in self.current_data.columns:
            print("[DEBUG] show_ppm: columna 'ppm_estimations' no encontrada")
            return

        def _normalize_row(row_dict):
            out = {}
            for metal in metales:
                val = None
                try:
                    if isinstance(row_dict, dict):
                        raw = row_dict.get(metal)
                        if raw is None:
                            val = 0
                        elif isinstance(raw, dict):
                            if "ppm" in raw and raw["ppm"] is not None:
                                val = float(raw["ppm"])
                            elif "pct_of_limit" in raw and raw["pct_of_limit"] is not None:
                                limit = float(self.limites_ppm.get(metal, 1))
                                val = float(raw["pct_of_limit"]) / 100.0 * limit
                            else:
                                val = 0
                        else:
                            val = float(raw)
                    else:
                        val = 0
                except Exception:
                    val = 0

                # Normalizar si viene exageradamente alto (ej. 10000 ppm → 10 ppm)
                limit = float(self.limites_ppm.get(metal, 1))
                if val > limit * 100:
                    val = val / 1000.0

                out[metal] = val
            return out

        rows = [_normalize_row(x) for x in self.current_data["ppm_estimations"]]
        df = pd.DataFrame(rows, columns=metales).fillna(0)
        self.ppm_df = df

        # ── CORRECCIÓN PRINCIPAL ─────────────────────────────────────────────
        # Tkinter no permite reasignar columnas a un Treeview ya creado.
        # Destruimos el widget existente y lo recreamos con las columnas correctas.
        if not hasattr(self, "_ppm_tree_container"):
            print("[ERROR] No existe _ppm_tree_container.")
            return

        # Destruir treeview viejo
        if hasattr(self, "tree_ppm"):
            try:
                self.tree_ppm.destroy()
            except Exception:
                pass

        # Recrear con las columnas exactas de los metales
        self.tree_ppm = ttk.Treeview(
            self._ppm_tree_container,
            columns=metales,
            show="headings",
            height=8,
        )
        for metal in metales:
            self.tree_ppm.heading(metal, text=metal)
            self.tree_ppm.column(metal, anchor="center", width=100)

        self.tree_ppm.pack(fill="both", expand=True)
        # ────────────────────────────────────────────────────────────────────

        # Insertar filas con resaltado según los límites
        for _, row in df.iterrows():
            alerta = False
            for metal in metales:
                val = row[metal]
                limit = float(self.limites_ppm.get(metal, float("inf")))
                if val > limit:
                    alerta = True
                    break

            tag = "alert" if alerta else "safe"
            formatted_values = [f"{row[m]:.3f}" for m in metales]
            self.tree_ppm.insert("", "end", values=formatted_values, tags=(tag,))

        # Estilos visuales
        self.tree_ppm.tag_configure("alert", background="#ffebee", foreground="#c62828")
        self.tree_ppm.tag_configure("safe", background="#e8f5e9", foreground="#2e7d32")

        ToolTip(
            self.tree_ppm,
            "Tabla de estimaciones PPM por metal.\nRojo: supera límite permitido.\nVerde: segura."
        )

        print("[DEBUG] show_ppm completado con éxito")

    # ————— Bloque: Botón "Mostrar Clasificación" —————
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

    # ————— Bloque: Cargar archivo y guardar en BD —————
    def seleccionar_archivo(self):
        """
        Abre un diálogo para seleccionar un archivo .pssession.
        Procesa la sesión, calcula niveles de contaminación según límites PPM,
        guarda todo en la base de datos y actualiza la interfaz.
        """
        print("[DEBUG] load_file() invoked")
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if not path:
            print("[DEBUG] Carga de archivo cancelada por el usuario")
            return

        try:
            print(f"[DEBUG] Procesando archivo: {path}")

            # 1️⃣ Importar funciones necesarias
            from pstrace_session import extract_session_dict, cargar_limites_ppm as cargar_limites
            print("[DEBUG] Módulo pstrace_session importado correctamente")

            # 2️⃣ Cargar límites de contaminación desde JSON
            limites = cargar_limites()
            print(f"[DEBUG] Límites PPM cargados: {list(limites.keys()) if limites else 'No disponibles'}")

            # 3️⃣ Extraer datos de sesión
            data = extract_session_dict(path)
            if not data:
                raise ValueError("No se extrajeron datos de la sesión")
            print("[DEBUG] Datos de sesión extraídos correctamente")

            # 4️⃣ Intentar guardar en base de datos (modo seguro)
            conn = None
            try:
                conn = pg8000.connect(**DB_CONFIG)
                cur = conn.cursor()
                fname = os.path.basename(path)
                now = datetime.datetime.now()

                # Insertar sesión
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

                # 5️⃣ Calcular contamination_level y classification_group por medición
                limits_path = os.path.join(os.path.dirname(__file__), ".", "limits_ppm.json")
                try:
                    with open(limits_path, "r", encoding="utf-8") as fh:
                        limits_data = json.load(fh)
                except Exception:
                    limits_data = {}
                    log.warning("No se pudo cargar limits_ppm.json; se usará modo sin límites por metal.")

                for idx, m in enumerate(data["measurements"]):
                    pca_key = "pca_scores" if "pca_scores" in m else ("pca_data" if "pca_data" in m else None)
                    pca_val = m.get(pca_key) if pca_key else None

                    # Extraer estimaciones PPM
                    ppm_vals = []
                    estim = m.get("ppm_estimations", {}) or {}

                    if isinstance(estim, dict) and estim:
                        for metal, limit in limits_data.items():
                            val = None
                            try:
                                metal_entry = estim.get(metal)
                                if isinstance(metal_entry, dict):
                                    if metal_entry.get("ppm") is not None:
                                        val = float(metal_entry["ppm"])
                                    elif metal_entry.get("pct_of_limit") is not None:
                                        val = float(metal_entry["pct_of_limit"]) / 100.0 * float(limit)
                                else:
                                    if metal_entry is not None:
                                        val = float(metal_entry)
                            except Exception:
                                val = None
                            if val is not None:
                                ppm_vals.append((metal, val))
                    else:
                        for metal, limit in limits_data.items():
                            try:
                                v = m.get(metal)
                                if v is not None:
                                    ppm_vals.append((metal, float(v)))
                            except Exception:
                                continue

                    cont_level = max([v for (_, v) in ppm_vals], default=0.0)

                    group = 0
                    if ppm_vals and limits_data:
                        exceed_lim = any(val > float(limits_data.get(met, float("inf"))) for met, val in ppm_vals)
                        exceed_warn = any(val > 0.5 * float(limits_data.get(met, float("inf"))) for met, val in ppm_vals)
                        if exceed_lim:
                            group = 1
                        elif exceed_warn:
                            group = 2
                    else:
                        thr = float(self.settings.get("alert_threshold", 0.5))
                        thr_anom = float(self.settings.get("anom_threshold", thr * 0.5))
                        if cont_level > thr:
                            group = 1
                        elif cont_level > thr_anom:
                            group = 2

                    try:
                        cur.execute(
                            """
                            INSERT INTO measurements
                            (session_id, title, timestamp, device_serial, curve_count,
                            pca_scores, contamination_level, classification_group)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                sid,
                                m.get("title"),
                                m.get("timestamp"),
                                m.get("device_serial"),
                                m.get("curve_count"),
                                pca_val,
                                cont_level,
                                group,
                            ),
                        )
                        log.debug(
                            "Medición %d insertada: contamination_level=%s, group=%s",
                            idx + 1, cont_level, group
                        )
                    except Exception as e:
                        conn.rollback()
                        print(f"[⚠️] No se pudo insertar medición (modo offline). Error: {e}")
                        log.warning("Fallo de inserción en BD: %s", e)

                conn.commit()
                print("[DEBUG] Datos guardados correctamente en BD")

            except Exception as db_error:
                print(f"[⚠️] No se pudo conectar o guardar en BD. Modo local activado. Error: {db_error}")
                sid = "LOCAL"
                log.warning("Fallo conexión BD: %s", db_error)

            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass

            # 6️⃣ Actualizar interfaz
            self.current_data = pd.DataFrame(data["measurements"])
            self.session_info = data["session_info"]
            self.session_info["session_id"] = sid

            self.log_message(f"Sesión {sid} cargada exitosamente.")

            # Poblar vista estructurada de detalle
            if hasattr(self, "_populate_detail_view"):
                self._populate_detail_view(self.session_info)

            indices = list(self.current_data.index)
            self.cmb_curve["values"] = indices
            if indices:
                self.cmb_curve.set(indices[0])

            self.show_curve()
            self.show_pca()
            self.show_ppm()
            self.load_sessions()
            print("[DEBUG] Interfaz actualizada correctamente")

        except Exception as e:
            print(f"[ERROR] Error en load_file: {str(e)}")
            import traceback
            traceback.print_exc()
            self.log_message(f"Error carga archivo: {e}")

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
        curvas = [arrs[i * n: (i + 1) * n] for i in range(len(self.settings["cycles"]))]
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

    # ————— Bloque: Mostrar PCA y varianza — ORIGINAL RESTAURADO —————
    def show_pca(self):
        """
        Calcula y muestra el PCA de los vectores pca_scores de todas las mediciones.
        Si existe un PCA entrenado (models/pca.pkl), lo usa para mostrar su varianza real.
        """
        print("[DEBUG] show_pca() invoked")
        if self.current_data is None or "pca_scores" not in self.current_data.columns:
            print("[DEBUG] show_pca: sin datos")
            return

        # Matriz de datos
        raw = self.current_data["pca_scores"].dropna()
        valid = [x for x in raw if isinstance(x, (list, tuple)) and len(x) > 0]
        if not valid:
            print("[DEBUG] show_pca: no hay vectores pca_scores validos")
                    return
                                    df = pd.DataFrame(valid).fillna(0)

        # === Bloque: Cargar PCA entrenado ===
        try:
            from pathlib import Path
            import joblib

            pca_path = Path(__file__).resolve().parents[1] / "models" / "pca.pkl"
            if pca_path.exists():
                pca = joblib.load(pca_path)
                print(f"[DEBUG] PCA cargado desde {pca_path}")
                var = pca.explained_variance_ratio_.cumsum() * 100
            else:
                print("[WARNING] No se encontró el PCA entrenado. Recalculando localmente...")
                pca = PCA().fit(df)
                var = pca.explained_variance_ratio_.cumsum() * 100
        except Exception as e:
            print(f"[ERROR] No se pudo cargar el PCA entrenado: {e}")
            pca = PCA().fit(df)
            var = pca.explained_variance_ratio_.cumsum() * 100

        # Limpiar ejes
        self.ax_pca.clear()         self.ax_pca.set_facecolor(COLOR_BG)         self.ax_pca.tick_params(colors="white")

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

    # ————— Bloque: Seleccionar sesión en tabla —————
    def on_session_select(self):
        print("[DEBUG] on_session_select() invoked")
        sel = self.tree.selection()
        if not sel:
            return

        item = self.tree.item(sel[0])
        values = item["values"]
        if not values or values[0] == "--":
            return

        sid = values[0]
        print(f"[DEBUG] Sesión seleccionada: ID={sid}")

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT s.filename, s.loaded_at, s.scan_rate, s.start_potential,
                       s.end_potential, s.software_version,
                       m.device_serial, m.curve_count,
                       m.contamination_level, m.classification_group
                FROM sessions s
                JOIN measurements m ON s.id = m.session_id
                WHERE s.id = %s
                LIMIT 1
                """,
                (sid,)
            )
            row = cur.fetchone()
            conn.close()

            if row:
                # Actualizar meta labels en pestaña Consultas
                if hasattr(self, "meta_labels"):
                    self.meta_labels["scan_rate"].config(text=f"Velocidad de Escaneo: {row[2] or '--'}")
                    self.meta_labels["start_potential"].config(text=f"Potencial Inicial: {row[3] or '--'}")
                    self.meta_labels["end_potential"].config(text=f"Potencial Final: {row[4] or '--'}")
                    self.meta_labels["software_version"].config(text=f"Versión Software: {row[5] or '--'}")

                # Construir dict para vista de detalle estructurada
                info = {
                    "session_id":           sid,
                    "filename":             row[0] or "--",
                    "loaded_at":            str(row[1]) if row[1] else "--",
                    "start_potential":      row[3] or "--",
                    "end_potential":        row[4] or "--",
                    "software_version":     row[5] or "--",
                    "device_serial":        row[6] or "--",
                    "curve_count":          row[7] or "--",
                    "contamination_level":  f"{float(row[8]):.4f}" if row[8] is not None else "--",
                    "classification_group": row[9] or "--",
                }

                # Poblar vista estructurada (sin scan_rate)
                if hasattr(self, "_populate_detail_view"):
                    self._populate_detail_view(info)

        except Exception as e:
            print(f"[ERROR] on_session_select: {e}")

    # ————— Bloque: Limpiar filtros —————
    def clear_filters(self):
        print("[DEBUG] clear_filters() invoked")
        if hasattr(self, "id_entry"):
            self.id_entry.delete(0, "end")
        if hasattr(self, "device_combobox"):
            self.device_combobox.current(0)
        self.set_default_date_range()

    # ————— Bloque: Cargar lista de sesiones —————
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
