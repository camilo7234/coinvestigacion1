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

# Importación de utilidades canónicas (Aseguran exactitud en métricas)
from canonical import (
    normalize_classification,
    display_label_from_label,
    classification_group_from_label,
    label_from_group
)

# ———————————————————————————————————————————————————————————————————————————
# BLOQUE: CONFIGURACIÓN GLOBAL
# ———————————————————————————————————————————————————————————————————————————
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

# Rutas absolutas para evitar errores de ejecución
_base_dir = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(os.path.dirname(_base_dir), "settings.json")
DEFAULT_SETTINGS = {"cycles": [2, 3, 4, 5], "ppm_factor": 1.0, "alert_threshold": 0.5}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# ———————————————————————————————————————————————————————————————————————————
# CLASES AUXILIARES
# ———————————————————————————————————————————————————————————————————————————
class ToolTip:
    """Crea una ayuda emergente (tooltip) para un widget."""
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tipwin = None
        widget.bind("<Enter>", self.show)
        widget.bind("<Leave>", self.hide)

    def show(self, _event):
        if self.tipwin or not self.text:
            return
        x, y, _, _ = self.widget.bbox("insert") if hasattr(self.widget, "bbox") and self.widget.winfo_class() in ("Text", "Entry") else (0,0,0,0)
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 25
        self.tipwin = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        tk.Label(tw, text=self.text, justify="left", background="#ffffe0", relief="solid", borderwidth=1, font=("Arial", "8")).pack(ipadx=1)

    def hide(self, _event):
        if self.tipwin:
            self.tipwin.destroy()
            self.tipwin = None

# ———————————————————————————————————————————————————————————————————————————
# APLICACIÓN PRINCIPAL
# ———————————————————————————————————————————————————————————————————————————
class Aplicacion(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sistema de Monitoreo Electroquímico - PROYECTO DE GRADO")
        self.geometry("1400x900")
        self.configure(bg=COLOR_BG)

        self.current_data = None
        self.session_info = {}
        self.settings = self.load_settings()

        self.setup_style()
        self.create_menu()
        self.create_tabs()
        self.load_devices()

    # ———————————————————————————————————————————————————————————————————————
    # GESTIÓN DE CONFIGURACIÓN
    # ———————————————————————————————————————————————————————————————————————
    def load_settings(self):
        if not os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "w") as f:
                    json.dump(DEFAULT_SETTINGS, f, indent=2)
            except: pass
            return DEFAULT_SETTINGS.copy()
        try:
            return json.load(open(SETTINGS_FILE))
        except:
            return DEFAULT_SETTINGS.copy()

    def save_settings(self):
        try:
            with open(SETTINGS_FILE, "w") as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            log.error(f"Error guardando settings: {e}")

    # ———————————————————————————————————————————————————————————————————————
    # UI HELPERS (AYUDAS RÁPIDAS)
    # ———————————————————————————————————————————————————————————————————————
    def _make_quick_help(self, parent, text):
        """Panel de ayuda rápida con diseño profesional."""
        help_frame = tk.LabelFrame(parent, text="ℹ️ AYUDA RÁPIDA", font=("Arial", 10, "bold"), 
                                  bg="#1a252f", fg="#3498db", labelanchor="nw", padx=10, pady=8)
        help_frame.pack(fill="x", side="top", padx=15, pady=10)
        
        lbl = tk.Label(help_frame, text=text, font=("Arial", 9, "italic"), 
                       bg="#1a252f", fg="#bdc3c7", justify="left", wraplength=1200)
        lbl.pack(anchor="w")

    def setup_style(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=COLOR_BG)
        s.configure("TButton", background=COLOR_BT, foreground=COLOR_FG, font=("Arial", 10, "bold"))
        s.configure("TLabel", background=COLOR_BG, foreground=COLOR_FG, font=("Arial", 10))
        s.configure("Treeview", background="#34495e", fieldbackground="#34495e", foreground=COLOR_FG)
        s.configure("TNotebook", background=COLOR_BG)
        s.configure("TNotebook.Tab", background="#34495e", foreground="#ecf0f1", padding=[10, 5])

    def create_menu(self):
        m = tk.Menu(self)
        f = tk.Menu(m, tearoff=0)
        f.add_command(label="Ajustes", command=self.show_settings)
        f.add_separator()
        f.add_command(label="Salir", command=self.quit)
        m.add_cascade(label="Archivo", menu=f)
        self.config(menu=m)

    def show_settings(self):
        w = tk.Toplevel(self)
        w.title("Ajustes del Sistema")
        w.geometry("350x250")
        w.configure(bg="#ecf0f1")
        
        ttk.Label(w, text="Umbral de Alerta (ppm):", background="#ecf0f1", foreground="#2c3e50").pack(pady=(20, 5))
        e = ttk.Entry(w, justify="center")
        e.insert(0, str(self.settings.get("alert_threshold", 0.5)))
        e.pack(pady=5)
        
        def save():
            try:
                self.settings["alert_threshold"] = float(e.get())
                self.save_settings()
                w.destroy()
                messagebox.showinfo("Éxito", "Configuración actualizada correctamente.")
            except:
                messagebox.showerror("Error", "Por favor ingresa un número válido.")
        
        ttk.Button(w, text="Guardar Cambios", command=save).pack(pady=30)

    # ———————————————————————————————————————————————————————————————————————
    # ESTRUCTURA DE PESTAÑAS
    # ———————————————————————————————————————————————————————————————————————
    def create_tabs(self):
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
        f = ttk.Frame(parent)
        parent.add(f, text="📤 Cargar Datos")
        
        # AYUDA RÁPIDA (Texto solicitado exactamente)
        self._make_quick_help(f, "Aquí podrás subir y cargar de manera exitosa tu archivo .pssession para ser procesado, solo haz clic en seleccionar el archivo y navega hasta la ubicación donde lo tenga, selecciónalo y dale abrir y listo.")
        
        container = ttk.Frame(f, padding=40)
        container.pack()
        
        ttk.Button(container, text="📂 Seleccionar Archivo .pssession", command=self.load_file).pack(pady=20)
        
        ttk.Label(f, text="Registro de Actividad:", font=("Arial", 10, "bold")).pack(anchor="w", padx=25)
        self.log_text = tk.Text(f, height=12, bg="#1e272e", fg="#2ecc71", font=("Courier", 10), padx=10, pady=10)
        self.log_text.pack(fill="x", padx=25, pady=(5, 20))

    def build_query_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🔍 Consultas")
        
        # AYUDA RÁPIDA (Texto solicitado exactamente)
        self._make_quick_help(f, "Aquí puedes filtrar por fecha, por el código ID del archivo y hacer otras consultas; explora un poco para darte cuenta de las funcionalidades.")

        # Filtros
        filt = ttk.LabelFrame(f, text="Filtros de Búsqueda", padding=15)
        filt.pack(fill="x", padx=15, pady=5)
        
        ttk.Label(filt, text="Desde:").grid(row=0, column=0)
        self.date_start = DateEntry(filt, date_pattern="yyyy-mm-dd", width=12)
        self.date_start.grid(row=0, column=1, padx=5)
        
        ttk.Label(filt, text="Hasta:").grid(row=0, column=2)
        self.date_end = DateEntry(filt, date_pattern="yyyy-mm-dd", width=12)
        self.date_end.grid(row=0, column=3, padx=5)

        ttk.Label(filt, text="Dispositivo:").grid(row=0, column=4, padx=(15, 0))
        self.device_cmb = ttk.Combobox(filt, state="readonly", width=15)
        self.device_cmb.grid(row=0, column=5, padx=5)

        ttk.Button(filt, text="🔎 Ejecutar Consulta", command=self.query_sessions).grid(row=0, column=6, padx=20)

        # Tabla de Resultados
        cols = ("ID", "Archivo", "Fecha", "Sensor", "Estado de Muestra", "Contaminación (%)")
        self.tree = ttk.Treeview(f, columns=cols, show="headings", height=18)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor="center", width=150)
        
        scroll = ttk.Scrollbar(f, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        
        self.tree.pack(fill="both", expand=True, side="left", padx=(15, 0), pady=15)
        scroll.pack(side="right", fill="y", padx=(0, 15), pady=15)
        
        self.tree.bind("<<TreeviewSelect>>", self.on_session_select)

        # Estilos Semáforo para la tabla
        self.tree.tag_configure("alert", background="#f8d7da", foreground="#721c24")
        self.tree.tag_configure("warning", background="#fff3cd", foreground="#856404")
        self.tree.tag_configure("safe", background="#d4edda", foreground="#155724")

    def build_detail_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📝 Detalle Sesión")
        
        # AYUDA RÁPIDA (Texto solicitado exactamente)
        self._make_quick_help(f, "Esta pestaña carga la información del archivo de la muestra que se acaba de analizar en el sistema, presentando los datos técnicos y resultados de clasificación de manera estructurada.")

        # --- MEJORA VISUAL: SECCIONES DIVIDIDAS ---
        top_frame = ttk.LabelFrame(f, text="Información General de la Sesión", padding=15)
        top_frame.pack(fill="x", padx=15, pady=5)
        
        self.lbl_session_id = ttk.Label(top_frame, text="ID Sesión: ---", font=("Arial", 10, "bold"))
        self.lbl_session_id.grid(row=0, column=0, padx=20)
        
        self.lbl_session_file = ttk.Label(top_frame, text="Archivo: ---", font=("Arial", 10, "bold"))
        self.lbl_session_file.grid(row=0, column=1, padx=20)

        # Tabla de Detalles de Metales y Muestras
        bottom_frame = ttk.LabelFrame(f, text="Resultados Detallados por Muestra", padding=15)
        bottom_frame.pack(fill="both", expand=True, padx=15, pady=10)

        cols = ("Muestra / Metal", "Resultado Obtenido", "Unidad", "Estado Final")
        self.tree_detail = ttk.Treeview(bottom_frame, columns=cols, show="headings")
        for c in cols:
            self.tree_detail.heading(c, text=c)
            self.tree_detail.column(c, anchor="center", width=200)
        
        self.tree_detail.pack(fill="both", expand=True)

        # Tags de color para el estado
        self.tree_detail.tag_configure("SEGURA", background="#d4edda", foreground="#155724")
        self.tree_detail.tag_configure("ANOMALA", background="#fff3cd", foreground="#856404")
        self.tree_detail.tag_configure("CONTAMINADA", background="#f8d7da", foreground="#721c24")

    def build_curve_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📊 Curvas")
        self._make_quick_help(f, "Representación gráfica de las curvas de voltametría cíclica obtenidas en la sesión actual.")
        
        self.fig_curve, self.ax_curve = plt.subplots(figsize=(10, 5), facecolor=COLOR_BG)
        self.ax_curve.set_facecolor("#1e272e")
        self.canvas_curve = FigureCanvasTkAgg(self.fig_curve, master=f)
        self.canvas_curve.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=15)

    def build_pca_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📈 PCA")
        self._make_quick_help(f, "Análisis de Componentes Principales para la detección de metales pesados y anomalías estadísticas.")
        
        self.fig_pca, self.ax_pca = plt.subplots(figsize=(10, 5), facecolor=COLOR_BG)
        self.ax_pca.set_facecolor("#1e272e")
        self.canvas_pca = FigureCanvasTkAgg(self.fig_pca, master=f)
        self.canvas_pca.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=15)

    def build_ppm_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🗂 ppm")
        self._make_quick_help(f, "Estimación cuantitativa en partes por millón (ppm) de los metales pesados identificados en el agua.")
        
        self.tree_ppm = ttk.Treeview(f, columns=("Metal", "Concentración", "Límite Máximo", "Exceso"), show="headings")
        for c in ("Metal", "Concentración", "Límite Máximo", "Exceso"):
            self.tree_ppm.heading(c, text=c)
            self.tree_ppm.column(c, anchor="center")
        self.tree_ppm.pack(fill="both", expand=True, padx=20, pady=20)

    def build_iot_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🌐 IoT")
        # Esta ya tenía ayuda rápida pero la unificamos al nuevo estilo
        self._make_quick_help(f, "Módulo de comunicación IoT para el envío de reportes y sincronización de datos con el servidor central.")
        
        tk.Label(f, text="Servidor de Comunicaciones Activo", font=("Arial", 14, "bold"), pady=20).pack()
        tk.Label(f, text="Esperando paquetes entrantes...", fg="#2ecc71", bg=COLOR_BG).pack()

    # ———————————————————————————————————————————————————————————————————————
    # LÓGICA DE PROCESAMIENTO
    # ———————————————————————————————————————————————————————————————————————
    def load_file(self):
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if not path: return
        try:
            from pstrace_session import extract_session_dict
            from db_persistence import guardar_sesion_y_mediciones

            self.log_message(f"--- Iniciando análisis de: {os.path.basename(path)} ---")
            data = extract_session_dict(path)
            if not data: raise Exception("No se encontraron datos válidos en el archivo.")

            sid = guardar_sesion_y_mediciones(data["session_info"], data["measurements"])
            
            self.session_info = data["session_info"]
            self.session_info["id"] = sid
            self.current_data = pd.DataFrame(data["measurements"])

            # Actualizar toda la interfaz con los nuevos datos
            self.update_detail_view()
            self.show_curve()
            self.query_sessions() # Refrescar tabla de consultas
            
            self.log_message(f"✅ Análisis completado con éxito. Sesión ID: {sid}")
            messagebox.showinfo("Procesamiento Exitoso", f"Los datos han sido analizados y guardados en la BD. ID de Sesión: {sid}")
        except Exception as e:
            self.log_message(f"❌ ERROR: {e}")
            messagebox.showerror("Error de Procesamiento", str(e))

    def update_detail_view(self):
        """Muestra los datos de la sesión de forma estructurada y profesional."""
        self.tree_detail.delete(*self.tree_detail.get_children())
        si = self.session_info
        
        # Actualizar labels de cabecera
        self.lbl_session_id.config(text=f"ID Sesión: {si.get('id', 'N/A')}")
        self.lbl_session_file.config(text=f"Archivo: {si.get('filename', 'N/A')}")

        # Insertar metadatos técnicos
        self.tree_detail.insert("", "end", values=("Software Version", si.get("software_version"), "-", "-"))
        self.tree_detail.insert("", "end", values=("Scan Rate", si.get("scan_rate"), "V/s", "-"))
        
        # Insertar resultados de cada muestra analizada
        if self.current_data is not None:
            for _, m in self.current_data.iterrows():
                # Normalización profesional de etiquetas
                canon = normalize_classification(m.get("clasificacion", "SEGURA"))
                estado_texto = display_label_from_label(canon)
                nivel = m.get("contamination_level", 0)
                
                self.tree_detail.insert("", "end", values=(
                    f"MUESTRA: {m.get('title')}", 
                    f"{nivel:.2f}", 
                    "%", 
                    estado_texto
                ), tags=(canon,))

    def query_sessions(self):
        try:
            start = self.date_start.get_date()
            end = self.date_end.get_date()
            dev = self.device_cmb.get()

            sql = """
                SELECT s.id, s.filename, s.loaded_at::date, m.device_serial,
                       m.classification_group, m.contamination_level
                FROM sessions s
                JOIN measurements m ON s.id = m.session_id
                WHERE s.loaded_at::date BETWEEN %s AND %s
            """
            params = [start, end]
            if dev and dev != "TODOS":
                sql += " AND m.device_serial = %s"
                params.append(dev)
            sql += " ORDER BY s.id DESC"

            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            conn.close()

            self.tree.delete(*self.tree.get_children())
            for r in rows:
                group = int(r[4])
                # Mapeo exacto según canonical.py (0=SEGURA, 1=ANOMALA, 2=CONTAMINADA)
                if group == 2: tag, txt = "alert", "CONTAMINADA"
                elif group == 1: tag, txt = "warning", "ANOMALA"
                else: tag, txt = "safe", "SEGURA"

                self.tree.insert("", "end", values=(r[0], r[1], r[2], r[3], txt, f"{r[5]:.2f}%"), tags=(tag,))
        except Exception as e:
            log.error(f"Error en consulta: {e}")

    def load_devices(self):
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT device_serial FROM measurements")
            devs = [r[0] for r in cur.fetchall()]
            conn.close()
            self.device_cmb["values"] = ["TODOS"] + devs
            self.device_cmb.current(0)
        except: pass

    def on_session_select(self, _=None):
        sel = self.tree.selection()
        if not sel: return
        sid = self.tree.item(sel[0])["values"][0]
        self.log_message(f"Consultando detalles técnicos para la sesión ID: {sid}")

    def show_curve(self):
        if self.current_data is None: return
        self.ax_curve.clear()
        self.ax_curve.set_facecolor("#1e272e")
        self.ax_curve.set_title("Voltagramas de la Sesión Actual", color="white", fontsize=12)
        self.ax_curve.set_xlabel("Puntos", color="white")
        self.ax_curve.set_ylabel("Corriente (A)", color="white")
        self.ax_curve.tick_params(colors="white")
        self.canvas_curve.draw()

    def log_message(self, msg):
        self.log_text.insert("end", f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}
")
        self.log_text.see("end")

if __name__ == "__main__":
    app = Aplicacion()
    app.mainloop()
