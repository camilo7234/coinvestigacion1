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

# ✅ CORRECCIÓN: Añadida clave "cycles"
DEFAULT_SETTINGS = {
    "selected_cycle": 3,
    "ppm_factor": 1.0,
    "alert_threshold": 0.5,
    "cycles": [3]  # ✅ Añadido para evitar KeyError
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()


class Aplicacion(tk.Tk):
    """
    Clase principal que representa la interfaz gráfica del Sistema de Monitoreo Electroquímico.
    """

    def __init__(self):
        super().__init__()
        self.title("Sistema de Monitoreo Electroquímico")
        self.geometry("1400x900")
        self.configure(bg=COLOR_BG)

        self.current_data = None
        self.session_info = {}
        self.ppm_df = None
        self.settings = self.load_settings()
        
        # ✅ CORRECCIÓN: Cargar límites al inicio
        self.limites_ppm = self._load_limits()

        self.setup_style()
        self.create_menu()
        self.create_tabs()
        self.load_sessions()

    # ✅ CORRECCIÓN: Nuevo método para cargar límites con fallback
    def _load_limits(self):
        """Carga límites desde JSON con fallback a valores por defecto"""
        local_path = os.path.join(os.path.dirname(__file__), "..", "limits_ppm.json")
        absolute_path = r"C:\Users\57321\OneDrive\Escritorio\GRADE\limits_ppm.json"
        
        for path in [local_path, absolute_path]:
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        limits = json.load(f)
                        log.info(f"Límites cargados desde {path}")
                        return limits
                except Exception as e:
                    log.warning(f"Error leyendo {path}: {e}")
        
        log.warning("Usando límites por defecto")
        return {"Cd": 0.10, "Zn": 3.00, "Cu": 1.00, "Cr": 0.50, "Ni": 0.50}

    def seleccionar_archivo(self):
        """Método de compatibilidad para load_file"""
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if path:
            self.load_file_internal(path)

    def load_file(self):
        """Llama al método de selección de archivo"""
        return self.seleccionar_archivo()

    def load_settings(self):
        """Carga ajustes desde JSON con validación de claves"""
        if not os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "w") as f:
                json.dump(DEFAULT_SETTINGS, f, indent=2)
            return DEFAULT_SETTINGS.copy()
        try:
            settings = json.load(open(SETTINGS_FILE))
            # ✅ CORRECCIÓN: Validar y añadir claves faltantes
            for key, value in DEFAULT_SETTINGS.items():
                if key not in settings:
                    settings[key] = value
            return settings
        except:
            log.warning("No se pudo leer settings.json; usando valores por defecto")
            return DEFAULT_SETTINGS.copy()

    def save_settings(self):
        """Guarda los ajustes actuales"""
        with open(SETTINGS_FILE, "w") as f:
            json.dump(self.settings, f, indent=2)
        log.info("Settings guardados: %s", self.settings)

    def setup_style(self):
        """Configura el estilo visual"""
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=COLOR_BG)
        s.configure("TButton", background=COLOR_BT, foreground=COLOR_FG, font=("Arial", 12, "bold"))
        s.map("TButton", background=[("active", "#2980b9")])
        s.configure("TLabel", background=COLOR_BG, foreground=COLOR_FG, font=("Arial", 11))
        s.configure("Treeview", background="#34495e", fieldbackground="#34495e", foreground=COLOR_FG)
        s.configure("TNotebook", background=COLOR_BG)
        s.configure("TNotebook.Tab", background="#7f8c8d", foreground=COLOR_FG, font=("Arial", 10, "bold"))

    def create_menu(self):
        """Crea el menú principal"""
        m = tk.Menu(self)
        f = tk.Menu(m, tearoff=0)
        f.add_separator()
        f.add_command(label="Salir", command=self.quit)
        m.add_cascade(label="Archivo", menu=f)
        h = tk.Menu(m, tearoff=0)
        h.add_command(label="Acerca de", command=lambda: messagebox.showinfo("Acerca de", "Sistema Monitoreo Electroquímico v2.0"))
        m.add_cascade(label="Ayuda", menu=h)
        self.config(menu=m)

    def create_tabs(self):
        """Crea las pestañas principales"""
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)
        self.build_load_tab(nb)
        self.build_query_tab(nb)
        self.build_detail_tab(nb)
        self.build_curve_tab(nb)
        self.build_pca_tab(nb)
        self.build_ppm_tab(nb)
        self.build_iot_tab(nb)

    def build_load_tab(self, parent):
        """Pestaña de carga de datos"""
        f = ttk.Frame(parent)
        parent.add(f, text="📤 Cargar Datos")
        ttk.Button(f, text="Seleccionar Archivo .pssession", command=self.load_file).pack(pady=20)
        self.log_text = tk.Text(f, height=8, bg="#34495e", fg="white", font=("Courier", 10))
        self.log_text.pack(fill="x", padx=10, pady=10)

    def show_settings_alternative(self):
        """Ventana de configuraciones"""
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
                messagebox.showinfo("Éxito", "Umbral guardado correctamente")
                settings_window.destroy()
            except ValueError:
                print("[DEBUG] save_settings() ValueError: invalid float")
                messagebox.showerror("Error", "El umbral debe ser un número válido")

        ttk.Button(button_frame, text="Guardar", command=save_settings).pack(side="right", padx=(5, 0))
        ttk.Button(button_frame, text="Cancelar", command=settings_window.destroy).pack(side="right")

    def build_query_tab(self, parent):
        """Pestaña de consultas"""
        print("[DEBUG] build_query_tab() invoked")
        tab = ttk.Frame(parent)
        parent.add(tab, text="🔍 Consultas")

        self._create_overview_panel(tab)
        self._create_filters_panel(tab)
        self._create_results_table(tab)
        self._create_meta_panel(tab)

        self.load_devices()
        self.update_overview()
        self.set_default_date_range()
        self.query_sessions()

    def _create_overview_panel(self, parent):
        """Panel de estadísticas generales"""
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
            ("min_ppm", "PPM Mínimo: --"),
            ("alert_count", "Alertas Activas: --"),
            ("last_update", "Última Actualización: --"),
        ]

        self.overview_labels = {}
        for i, (key, text) in enumerate(fields):
            r, c = divmod(i, 3)
            lbl = ttk.Label(container, text=text, font=("Arial", 10))
            lbl.grid(row=r, column=c, sticky="w", padx=20, pady=5)
            self.overview_labels[key] = lbl

        ttk.Button(container, text="🔄 Actualizar", command=self.update_overview).grid(row=3, column=0, columnspan=3, pady=10)

    def _create_filters_panel(self, parent):
        """Panel de filtros"""
        print("[DEBUG] _create_filters_panel() invoked")
        frame = ttk.LabelFrame(parent, text="Filtros de Búsqueda")
        frame.pack(fill="x", padx=10, pady=5)
        container = ttk.Frame(frame)
        container.pack(fill="x", padx=10, pady=10)

        ttk.Label(container, text="ID Sesión:").grid(row=0, column=0, sticky="e", padx=5)
        self.id_entry = ttk.Entry(container, width=10)
        self.id_entry.grid(row=0, column=1, padx=5)
        ToolTip(self.id_entry, "Introduce el ID de la sesión")

        ttk.Label(container, text="Fecha Inicio:").grid(row=0, column=2, sticky="e", padx=5)
        self.date_start = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_start.grid(row=0, column=3, padx=5)

        ttk.Label(container, text="Fecha Fin:").grid(row=0, column=4, sticky="e", padx=5)
        self.date_end = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_end.grid(row=0, column=5, padx=5)

        ttk.Label(container, text="Dispositivo:").grid(row=1, column=0, sticky="e", padx=5, pady=(10, 0))
        self.device_combobox = ttk.Combobox(container, state="readonly", width=12)
        self.device_combobox.grid(row=1, column=1, padx=5, pady=(10, 0))
        self.device_combobox["values"] = ["— Todos —"]
        self.device_combobox.current(0)

        btns = ttk.Frame(container)
        btns.grid(row=2, column=0, columnspan=6, pady=(10, 0))
        ttk.Button(btns, text="🔍 Buscar", command=self.query_sessions).pack(side="left", padx=5)
        ttk.Button(btns, text="🗑️ Limpiar", command=self.clear_filters).pack(side="left", padx=5)
        ttk.Button(btns, text="📅 Últimos 7d", command=self.set_default_date_range).pack(side="left", padx=5)

    def clear_filters(self):
        """Limpia los filtros de búsqueda"""
        print("[DEBUG] clear_filters() invoked")
        self.id_entry.delete(0, "end")
        self.device_combobox.current(0)
        self.set_default_date_range()

    def _create_results_table(self, parent):
        """Tabla de resultados"""
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

        self.tree.tag_configure("alert", background="#ffebee", foreground="#c62828")
        self.tree.tag_configure("safe", background="#e8f5e9", foreground="#2e7d32")
        self.tree.bind("<<TreeviewSelect>>", lambda ev: self.on_session_select())

    def on_session_select(self):
        """Maneja la selección de una sesión en el TreeView"""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        values = item['values']
        if values and len(values) > 0:
            session_id = values[0]
            print(f"[DEBUG] Sesión seleccionada: {session_id}")
            # Guardar ID de sesión actual
            self.current_session_id = session_id

    def _create_meta_panel(self, parent):
        """Panel de detalles técnicos"""
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

    # ✅ CORRECCIÓN: Consulta SQL arreglada sin columnas inexistentes
    def query_sessions(self):
        """Consulta sesiones con clasificación desde BD"""
        log.debug("🔄 Iniciando query_sessions()")

        sid_text = self.id_entry.get().strip()
        try:
            session_id = int(sid_text) if sid_text else None
        except ValueError:
            session_id = None

        start_date = self.date_start.get_date().strftime("%Y-%m-%d")
        end_date = self.date_end.get_date().strftime("%Y-%m-%d")
        device = self.device_combobox.get()
        use_device_filter = device and device != "— Todos —"

        params = [start_date, end_date]
        if session_id is not None:
            params.append(session_id)
        if use_device_filter:
            params.append(device)

        # ✅ CORRECCIÓN: SQL sin columnas inexistentes
        sql = """
            SELECT 
                s.id,
                s.filename,
                s.loaded_at::date AS fecha,
                m.device_serial AS dispositivo,
                m.curve_count AS curvas,
                COALESCE(ROUND(m.contamination_level::numeric, 2), 0) AS nivel_ppm,
                CASE 
                    WHEN m.classification_group = 1 THEN '⚠️ CONTAMINACIÓN ALTA'
                    WHEN m.classification_group = 2 THEN '⚡ CONTAMINACIÓN MEDIA'
                    ELSE '✅ SEGURO'
                END AS clasificacion,
                '{}'::jsonb AS ppm_estimations
            FROM sessions s
            JOIN measurements m ON s.id = m.session_id
            WHERE s.loaded_at::date BETWEEN %s AND %s
        """

        if session_id is not None:
            sql += " AND s.id = %s"
        if use_device_filter:
            sql += " AND m.device_serial = %s"

        sql += " ORDER BY fecha DESC, nivel_ppm DESC"

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error(f"❌ Error en consulta: {e}")
            messagebox.showerror("Error", f"No se pudo ejecutar la consulta:\n{e}")
            return

        self.tree.delete(*self.tree.get_children())

        if not rows:
            self.tree.insert("", "end", values=("--", "Sin resultados", "--", "--", "--", "--", "0.00", "--"))
            return

        for r in rows:
            nivel = float(r[5])
            clasificacion = r[6] or "--"
            ppm_json = r[7] or {}

            if clasificacion.startswith("⚠️"):
                tag = "alert"
            elif clasificacion.startswith("⚡"):
                tag = "warning"
            else:
                tag = "safe"

            valores = (r[0], r[1], r[2], r[3], r[4], clasificacion, nivel, json.dumps(ppm_json))
            self.tree.insert("", "end", values=valores, tags=(tag,))

        self.tree.tag_configure("alert", background="#ffcdd2", foreground="#d32f2f")
        self.tree.tag_configure("warning", background="#ffe0b2", foreground="#f57c00")
        self.tree.tag_configure("safe", background="#c8e6c9", foreground="#2e7d32")

        self.update_overview()

    def load_devices(self):
        """Carga dispositivos en combobox"""
        print("[DEBUG] load_devices() invoked")
        if not hasattr(self, "device_combobox"):
            return

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT device_serial FROM measurements WHERE device_serial IS NOT NULL ORDER BY device_serial")
            vals = [row[0] for row in cur.fetchall()]
            conn.close()

            options = ["— Todos —"] + vals if vals else ["— Todos —"]
            self.device_combobox["values"] = options
            self.device_combobox.current(0)
            self.device_combobox.bind("<<ComboboxSelected>>", lambda ev: self.query_sessions())
        except Exception as e:
            print(f"[DEBUG] load_devices Error: {e}")

    def update_overview(self):
        """Actualiza estadísticas generales"""
        print("[DEBUG] update_overview() invoked")
        if not hasattr(self, "overview_labels"):
            return

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()

            queries = {
                "total_sessions": "SELECT COUNT(*) FROM sessions",
                "total_measurements": "SELECT COUNT(*) FROM measurements",
                "avg_ppm": "SELECT ROUND(AVG(contamination_level)::numeric, 2) FROM measurements WHERE contamination_level IS NOT NULL",
                "max_ppm": "SELECT ROUND(MAX(contamination_level)::numeric, 2) FROM measurements WHERE contamination_level IS NOT NULL",
                "min_ppm": "SELECT ROUND(MIN(contamination_level)::numeric, 2) FROM measurements WHERE contamination_level IS NOT NULL",
                "alert_count": "SELECT COUNT(*) FROM measurements WHERE contamination_level >= %s",
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

            self.overview_labels["total_sessions"].config(text=f"Total de Sesiones: {stats['total_sessions']}")
            self.overview_labels["total_measurements"].config(text=f"Total de Mediciones: {stats['total_measurements']}")
            self.overview_labels["avg_ppm"].config(text=f"PPM Promedio: {stats['avg_ppm'] if stats['avg_ppm'] else '--'}")
            self.overview_labels["max_ppm"].config(text=f"PPM Máximo: {stats['max_ppm'] if stats['max_ppm'] else '--'}")
            self.overview_labels["min_ppm"].config(text=f"PPM Mínimo: {stats['min_ppm'] if stats['min_ppm'] else '--'}")
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
            messagebox.showerror("Error", f"Error actualizando vista:\n{e}")

    def set_default_date_range(self):
        """Establece rango de últimos 7 días"""
        print("[DEBUG] set_default_date_range() invoked")
        if not hasattr(self, "date_start") or not hasattr(self, "date_end"):
            return

        try:
            today = datetime.date.today()
            last7 = today - datetime.timedelta(days=7)
            self.date_start.set_date(last7)
            self.date_end.set_date(today)
            self.query_sessions()
        except Exception as e:
            print(f"[DEBUG] set_default_date_range Error: {e}")

    def build_detail_tab(self, parent):
        """Pestaña de detalle de sesión"""
        f = ttk.Frame(parent)
        parent.add(f, text="📝 Detalle Sesión")
        self.txt_detail = tk.Text(f, wrap="word", bg="#34495e", fg="white", font=("Arial", 10))
        self.txt_detail.pack(fill="both", expand=True, padx=10, pady=10)

    def build_curve_tab(self, parent):
        """Pestaña de curvas"""
        f = ttk.Frame(parent)
        parent.add(f, text="📊 Curvas")
        frm = ttk.Frame(f)
        frm.pack(fill="x", padx=10, pady=8)

        ttk.Label(frm, text="Índice medida:").pack(side="left", padx=5)
        self.cmb_curve = ttk.Combobox(frm, state="readonly", width=8)
        self.cmb_curve.pack(side="left", padx=5)
        self.cmb_curve.bind("<<ComboboxSelected>>", lambda e: self.show_curve())

        self.fig_curve, self.ax_curve = plt.subplots(figsize=(9, 5), facecolor=COLOR_BG)
        self.ax_curve.set_facecolor(COLOR_BG)
        self.ax_curve.tick_params(colors="white")
        self.ax_curve.grid(True, color="#5d6d7e")
        self.canvas_curve = FigureCanvasTkAgg(self.fig_curve, master=f)
        self.canvas_curve.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        ttk.Button(f, text="Exportar PNG", command=lambda: self.export_figure(self.fig_curve)).pack(side="right", padx=10, pady=5)

    def build_ppm_tab(self, parent):
        """Pestaña de clasificación PPM"""
        f = ttk.Frame(parent)
        parent.add(f, text="🗂 Clasificación")

        # Crear labels para mostrar valores
        info_frame = ttk.Frame(f)
        info_frame.pack(pady=10)
        
        self.lbl_maxppm = ttk.Label(info_frame, text="max.ppm: --", font=("Arial", 12, "bold"))
        self.lbl_maxppm.pack(side="left", padx=20)
        
        self.lbl_classification = ttk.Label(info_frame, text="Estado: --", font=("Arial", 12, "bold"))
        self.lbl_classification.pack(side="left", padx=20)

        ttk.Button(f, text="Mostrar Clasificación", command=self.show_classification).pack(pady=8)

        cols = ("Grupo", "Nivel (%)")
        self.tree_ppm = ttk.Treeview(f, columns=cols, show="headings", height=8)
        for c in cols:
            self.tree_ppm.heading(c, text=c)
            self.tree_ppm.column(c, anchor="center")
        self.tree_ppm.pack(fill="both", expand=True, padx=10, pady=10)

        ttk.Button(f, text="Exportar Clasificación", command=self.export_classification).pack(side="right", padx=10, pady=5)

    def export_classification(self):
        """Exporta tabla de clasificación a CSV"""
        if self.ppm_df is None or self.ppm_df.empty:
            messagebox.showwarning("Exportar", "No hay datos de clasificación para exportar.")
            return

        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return

        try:
            self.ppm_df.to_csv(path, index=False, encoding="utf-8-sig")
            messagebox.showinfo("Exportar", f"Clasificación guardada en {path}")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo exportar:\n{e}")

    def build_iot_tab(self, parent):
        """Pestaña IoT/Comunicación"""
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

        ttk.Label(f, text="ℹ️ Ayuda rápida:", font=("Arial", 11, "bold")).pack(pady=(10, 2))
        help_text = (
            "1️⃣ Para recibir archivos, inicia el servidor en el dispositivo de destino.\n"
            "2️⃣ En el otro dispositivo, introduce la IP del servidor.\n"
            "3️⃣ Usa 'Probar Conexión' para verificar.\n"
            "4️⃣ Si funciona, selecciona un archivo y presiona 'Enviar Archivo'.\n"
        )
        help_label = tk.Text(f, height=8, wrap="word", bg="#2c3e50", fg="#ecf0f1", font=("Arial", 9))
        help_label.insert("1.0", help_text)
        help_label.config(state="disabled")
        help_label.pack(fill="x", padx=15, pady=(0, 10))

    # ✅ CORRECCIÓN: log_iot con thread-safe después de after()
    def log_iot(self, msg):
        """Agrega texto a consola IoT de forma thread-safe"""
        def _update():
            self.iot_log.insert("end", msg + "\n")
            self.iot_log.see("end")
        self.after(0, _update)
        log.info("[IoT] " + msg)

    def start_iot_server(self):
        """Inicia servidor IoT"""
        if self.server_running:
            self.log_iot("⚠️ El servidor ya está en ejecución.")
            return

        def server_loop():
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
                                chunk = conn.recv(1)
                                if not chunk:
                                    break
                                header_data += chunk
                            if not header_data:
                                self.log_iot("⚠️ Conexión vacía.")
                                continue

                            header = json.loads(header_data.decode().strip())
                            filename = header["filename"]
                            size = int(header["size"])
                            checksum = header["checksum"]

                            filepath = os.path.join(dest_dir, filename)
                            conn.sendall(b"ACK")
                            with open(filepath, "wb") as f:
                                total_received = 0
                                while total_received < size:
                                    data = conn.recv(buffer_size)
                                    if not data:
                                        break
                                    f.write(data)
                                    total_received += len(data)
                            self.log_iot(f"✅ Archivo recibido: {filename} ({total_received/1e6:.2f} MB)")
                            conn.sendall(b"EOF_OK")

                    except socket.timeout:
                        continue
                    except Exception as e:
                        self.log_iot(f"❌ Error en servidor: {e}")
                        continue

            self.server_running = False
            self.log_iot("🛑 Servidor IoT detenido.")

        self.server_thread = threading.Thread(target=server_loop, daemon=True)
        self.server_thread.start()

    def stop_iot_server(self):
        """Detiene servidor IoT"""
        if not self.server_running:
            self.log_iot("⚠️ El servidor no está activo.")
            return
        self.server_running = False
        self.log_iot("🛑 Solicitando apagado del servidor...")

    def test_iot_connection(self):
        """Prueba conexión con servidor IoT"""
        host = self.iot_ip_var.get()
        port = self.iot_port_var.get()
        try:
            with socket.create_connection((host, port), timeout=3) as s:
                s.sendall(b"ping")
                self.log_iot(f"✅ Conectado con {host}:{port}")
                messagebox.showinfo("Conexión exitosa", f"Conectado con {host}:{port}")
        except Exception as e:
            self.log_iot(f"❌ Error al conectar: {e}")
            messagebox.showerror("Error de conexión", str(e))

    def send_iot_file(self):
        """Envía archivo al servidor IoT"""
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
            "checksum": checksum
        }).encode() + b"\n"

        try:
            with socket.create_connection((host, port)) as s:
                s.sendall(header)
                ack = s.recv(8)
                if ack != b"ACK":
                    raise Exception("Servidor no aceptó la transferencia")

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
                s.sendall(b"EOF")
                messagebox.showinfo("Éxito", f"Archivo {filename} enviado correctamente.")
        except Exception as e:
            self.log_iot(f"❌ Error de envío: {e}")
            messagebox.showerror("Error", str(e))

    def log_message(self, msg):
        """Registra mensajes en ventana de log"""
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        log.info(msg)

    def export_figure(self, fig):
        """Exporta figura a PNG"""
        path = filedialog.asksaveasfilename(defaultextension=".png", filetypes=[("PNG", "*.png")])
        if path:
            fig.savefig(path)
            messagebox.showinfo("Exportar", f"Guardado en {path}")

    def export_ppm(self):
        """Exporta tabla PPM a CSV"""
        if self.ppm_df is None:
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if path:
            self.ppm_df.to_csv(path, index=False)
            messagebox.showinfo("Exportar", f"Guardado en {path}")

    # ✅ CORRECCIÓN: show_ppm como MÉTODO de la clase
    def show_ppm(self):
        """Muestra valores PPM de la sesión actual"""
        if not hasattr(self, 'current_session_id') or not self.current_session_id:
            print("[DEBUG] No hay sesión actual para mostrar PPM")
            if hasattr(self, 'lbl_maxppm'):
                self.lbl_maxppm.config(text="max.ppm: —")
            if hasattr(self, 'lbl_classification'):
                self.lbl_classification.config(text="Clasificación: —")
            return

        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()

            print(f"[DEBUG] Obteniendo PPM para sesión {self.current_session_id}")

            cur.execute("""
                SELECT contamination_level, classification_group, pca_scores
                FROM measurements 
                WHERE session_id = %s
                ORDER BY contamination_level DESC
                LIMIT 1
            """, (self.current_session_id,))

            row = cur.fetchone()
            conn.close()

            if not row:
                print("[DEBUG] No se encontraron mediciones")
                if hasattr(self, 'lbl_maxppm'):
                    self.lbl_maxppm.config(text="max.ppm: —")
                if hasattr(self, 'lbl_classification'):
                    self.lbl_classification.config(text="Clasificación: —")
                return

            contamination_level, classification_group, pca_scores = row

            # Mostrar nivel de contaminación
            if hasattr(self, 'lbl_maxppm'):
                self.lbl_maxppm.config(text=f"max.ppm: {contamination_level:.2f}")

            # Mostrar clasificación
            if hasattr(self, 'lbl_classification'):
                if classification_group == 1:
                    clasificacion = "ALTA"
                elif classification_group == 2:
                    clasificacion = "MEDIA"
                else:
                    clasificacion = "SEGURA"
                self.lbl_classification.config(text=f"Estado: {clasificacion}")

            print("[DEBUG] PPM actualizado correctamente")

        except Exception as e:
            print(f"[ERROR] show_ppm(): {e}")
            if hasattr(self, 'lbl_maxppm'):
                self.lbl_maxppm.config(text="max.ppm: Error")
            if hasattr(self, 'lbl_classification'):
                self.lbl_classification.config(text="Estado: Error")

    def show_classification(self):
        """Muestra tabla de clasificación"""
        print("[DEBUG] show_classification() invoked")

        if self.current_data is None or self.current_data.empty:
            print("[DEBUG] No hay datos en current_data")
            messagebox.showwarning("Sin datos", "Carga primero un archivo .pssession")
            return

        rows = []
        for idx, m in self.current_data.iterrows():
            clasificacion = m.get("classification_group", 0)
            nivel = m.get("contamination_level", 0)

            if clasificacion == 1:
                estado = "⚠️ CONTAMINACIÓN ALTA"
            elif clasificacion == 2:
                estado = "⚡ CONTAMINACIÓN MEDIA"
            else:
                estado = "✅ SEGURO"

            rows.append({
                "Grupo": estado,
                "Nivel (%)": f"{nivel:.2f}%"
            })

        cols = ("Grupo", "Nivel (%)")
        self.tree_ppm.config(columns=cols)
        for c in cols:
            self.tree_ppm.heading(c, text=c)
            self.tree_ppm.column(c, anchor="center")

        self.tree_ppm.delete(*self.tree_ppm.get_children())
        self.ppm_df = pd.DataFrame(rows)

        for _, row in self.ppm_df.iterrows():
            estado = row["Grupo"]
            tag = "safe" if "SEGURO" in estado else "alert"
            vals = (row["Grupo"], row["Nivel (%)"])
            self.tree_ppm.insert("", "end", values=vals, tags=(tag,))

        self.tree_ppm.tag_configure("alert", background="#ffcdd2", foreground="#d32f2f")
        self.tree_ppm.tag_configure("safe", background="#c8e6c9", foreground="#2e7d32")

        print("[DEBUG] Tabla de clasificación actualizada")

    # ✅ CORRECCIÓN: load_file_internal como método
    def load_file_internal(self, path):
        """Procesa y carga archivo .pssession"""
        print(f"[DEBUG] Procesando archivo: {path}")

        try:
            from pstrace_session import extraer_y_procesar_sesion_completa
            print("[DEBUG] Módulo pstrace_session importado")

            # ✅ CORRECCIÓN: Pasar limites_ppm como argumento
            session_data = extraer_y_procesar_sesion_completa(path, self.limites_ppm)
            if not session_data:
                raise ValueError("No se extrajeron datos de la sesión")

            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            fname = os.path.basename(path)
            now = datetime.datetime.now()

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
                    session_data["session_info"].get("scan_rate"),
                    session_data["session_info"].get("start_potential"),
                    session_data["session_info"].get("end_potential"),
                    session_data["session_info"].get("software_version"),
                ),
            )
            sid = cur.fetchone()[0]
            print(f"[DEBUG] Sesión insertada. ID: {sid}")

            for idx, measurement in enumerate(session_data["measurements"]):
                contamination_level = measurement.get("contamination_level", 0.0)
                
                # ✅ CORRECCIÓN: Lógica de clasificación con límites realistas
                # Si contamination_level está en % respecto a límites oficiales:
                # - Nivel < 100% = SEGURO (no excede límites)
                # - Nivel 100-200% = MEDIA (1-2x el límite)
                # - Nivel > 200% = ALTA (>2x el límite)
                
                # PERO si los valores vienen en escala de sensor (µA), necesitamos normalizar
                # Detectar si está en escala errónea (>1000% indica escala incorrecta)
                if contamination_level > 1000:
                    # Aplicar factor de normalización (ajusta según tu calibración)
                    # Ejemplo: si el sensor da valores en rango 0-200 µA para 0-5 ppm
                    contamination_level_normalized = contamination_level / 1350  # Factor ajustable
                    print(f"[WARN] Valor fuera de escala: {contamination_level}% → normalizado a {contamination_level_normalized}%")
                    contamination_level = contamination_level_normalized
                
                # Clasificación con límites correctos
                if contamination_level >= 100:
                    classification_group = 1  # Alta (>2x límite)
                elif contamination_level >=65:
                    classification_group = 2  # Media (1-2x límite)
                else:
                    classification_group = 0  # Segura (<límite)

                pca_key = "pca_scores" if "pca_scores" in measurement else "pca_data"
                pca_scores = measurement.get(pca_key)

                # Convertir a lista Python para pg8000
                if pca_scores:
                    if isinstance(pca_scores, str):
                        pca_scores = json.loads(pca_scores)
                    pca_scores_array = list(pca_scores) if pca_scores else None
                else:
                    pca_scores_array = None

                print(f"[DEBUG] Medición {idx+1} -> nivel={contamination_level:.2f}%, grupo={classification_group}")

                cur.execute(
                    """
                    INSERT INTO measurements
                      (session_id, title, timestamp, device_serial, curve_count,
                       pca_scores, classification_group, contamination_level)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sid,
                        measurement.get("title"),
                        measurement.get("timestamp"),
                        measurement.get("device_serial"),
                        measurement.get("curve_count"),
                        pca_scores_array,
                        classification_group,
                        float(contamination_level),
                    ),
                )

            conn.commit()
            conn.close()
            print("[DEBUG] Datos guardados en BD")

            self.current_data = pd.DataFrame(session_data["measurements"])
            self.session_info = session_data["session_info"]
            self.session_info["session_id"] = sid
            self.current_session_id = sid

            self.log_message(f"Sesión {sid} cargada correctamente")
            
            self.txt_detail.delete("1.0", "end")
            self.txt_detail.insert("end", json.dumps(self.session_info, indent=2, ensure_ascii=False))

            indices = list(self.current_data.index)
            self.cmb_curve["values"] = indices
            if indices:
                self.cmb_curve.set(indices[0])

            self.show_curve()
            self.show_pca()
            self.show_classification()
            self.show_ppm()
            self.load_sessions()

        except Exception as e:
            print(f"[ERROR] Error en load_file_internal: {e}")
            import traceback
            traceback.print_exc()
            self.log_message(f"Error: {e}")
            messagebox.showerror("Error", f"Error al cargar:\n{e}")

    def query_sessions_alternative(self):
        """Consulta alternativa de sesiones"""
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
                   CASE WHEN m.contamination_level > %s THEN '⚠️ Contaminación' ELSE '✅ Limpio' END AS Estado
            FROM sessions s
            JOIN measurements m ON s.id = m.session_id
            WHERE 1=1
            {where_sql}
            GROUP BY s.id, s.filename, s.loaded_at, m.device_serial, m.curve_count, m.contamination_level
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

    def show_curve(self):
        """Muestra curvas de voltametría"""
        print("[DEBUG] show_curve() invoked")
        if self.current_data is None or self.cmb_curve.get() == "":
            return

        idx = int(self.cmb_curve.get())
        arrs = self.current_data.at[idx, "pca_scores"] if "pca_scores" in self.current_data.columns else []
        
        if not arrs:
            print("[DEBUG] No hay datos de pca_scores")
            return

        cycles = self.settings.get("cycles", [3])
        n = len(arrs) // len(cycles) if arrs and len(cycles) > 0 else len(arrs)
        curvas = [arrs[i * n : (i + 1) * n] for i in range(len(cycles))]
        x = list(range(n))

        self.ax_curve.clear()

        for i, curve in enumerate(curvas, start=1):
            color = "#95a5a6"
            alpha = 0.3
            linewidth = 1
            if i == 3:
                color = "#e74c3c"
                alpha = 1.0
                linewidth = 2
            self.ax_curve.plot(x, curve, color=color, alpha=alpha, linewidth=linewidth)

        si = self.session_info
        sensor = self.current_data.at[idx, "device_serial"] if "device_serial" in self.current_data.columns else "N/A"
        self.ax_curve.set_title(f"Sesión {si.get('session_id', 'N/A')} · Sensor {sensor}", color="white")
        self.ax_curve.set_xlabel("Índice de punto", color="white")
        self.ax_curve.set_ylabel("Corriente (A)", color="white")
        self.ax_curve.legend(["Curvas individuales", "Ciclo 3 (referencia)"], facecolor=COLOR_BG, labelcolor="white")
        self.ax_curve.grid(True, color="#5d6d7e")
        self.canvas_curve.draw()

    def show_pca(self):
        """Calcula y muestra PCA"""
        print("[DEBUG] show_pca() invoked")
        if self.current_data is None or "pca_scores" not in self.current_data.columns:
            return

        try:
            df = pd.DataFrame(self.current_data["pca_scores"].tolist()).fillna(0)
            if df.empty:
                return

            pca = PCA().fit(df)
            var = pca.explained_variance_ratio_.cumsum() * 1000

            self.ax_pca.clear()
            self.ax_pca.plot(range(1, len(var) + 1), var, marker="o", linewidth=2, color="#3498db")
            
            for i, v in enumerate(var[:3], start=1):
                self.ax_pca.annotate(f"{v:.1f}%", (i, v), textcoords="offset points", xytext=(0, 5), ha="center", color="white")

            self.ax_pca.set_ylim(0, 100)
            self.ax_pca.set_title("Varianza Acumulada PCA", color="white")
            self.ax_pca.set_xlabel("Componentes", color="white")
            self.ax_pca.set_ylabel("Varianza (%)", color="white")
            self.ax_pca.grid(True, color="#5d6d7e")
            self.canvas_pca.draw()

        except Exception as e:
            print(f"[DEBUG] show_pca Error: {e}")
            messagebox.showerror("Error", f"Error calculando PCA:\n{e}")

    def build_pca_tab(self, parent):
        """Pestaña PCA"""
        print("[DEBUG] build_pca_tab() invoked")
        f = ttk.Frame(parent)
        parent.add(f, text="📈 PCA")

        ttk.Button(f, text="Mostrar PCA", command=self.show_pca).pack(pady=8)

        self.fig_pca, self.ax_pca = plt.subplots(figsize=(9, 5), facecolor=COLOR_BG)
        self.ax_pca.set_facecolor(COLOR_BG)
        self.ax_pca.tick_params(colors="white")
        self.ax_pca.grid(True, color="#5d6d7e")

        self.canvas_pca = FigureCanvasTkAgg(self.fig_pca, master=f)
        self.canvas_pca.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        ttk.Button(f, text="Exportar PCA", command=lambda: self.export_figure(self.fig_pca)).pack(side="right", padx=10, pady=5)

    def load_sessions(self):
        """Carga lista de sesiones"""
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT id FROM sessions")
            _ = [r[0] for r in cur.fetchall()]
            conn.close()
        except Exception as e:
            self.log_message(f"Error cargando sesiones: {e}")


if __name__ == "__main__":
    app = Aplicacion()
    app.mainloop()