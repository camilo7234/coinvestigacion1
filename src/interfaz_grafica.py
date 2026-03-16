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

# Importación de utilidades canónicas
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

# Ruta de settings absoluta
SETTINGS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "settings.json"))
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
        self.title("Sistema de Monitoreo Electroquímico - RAMA OPTIMIZACIONES")
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
            with open(SETTINGS_FILE, "w") as f:
                json.dump(DEFAULT_SETTINGS, f, indent=2)
            return DEFAULT_SETTINGS.copy()
        try:
            return json.load(open(SETTINGS_FILE))
        except:
            return DEFAULT_SETTINGS.copy()

    def save_settings(self):
        with open(SETTINGS_FILE, "w") as f:
            json.dump(self.settings, f, indent=2)

    # ———————————————————————————————————————————————————————————————————————
    # UI HELPERS
    # ———————————————————————————————————————————————————————————————————————
    def _make_quick_help(self, parent, text):
        """Inyecta un panel de ayuda rápida al inicio de una pestaña."""
        f = tk.Frame(parent, bg="#1a252f", padx=10, pady=8)
        f.pack(fill="x", side="top")
        tk.Label(f, text="ℹ️ AYUDA RÁPIDA", font=("Arial", 9, "bold"), bg="#1a252f", fg="#3498db").pack(anchor="w")
        tk.Label(f, text=text, font=("Arial", 9), bg="#1a252f", fg="#bdc3c7", justify="left").pack(anchor="w")

    def setup_style(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=COLOR_BG)
        s.configure("TButton", background=COLOR_BT, foreground=COLOR_FG, font=("Arial", 10, "bold"))
        s.configure("TLabel", background=COLOR_BG, foreground=COLOR_FG, font=("Arial", 10))
        s.configure("Treeview", background="#34495e", fieldbackground="#34495e", foreground=COLOR_FG)
        s.configure("TNotebook", background=COLOR_BG)

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
        w.title("Ajustes")
        w.geometry("300x200")
        ttk.Label(w, text="Umbral Alerta (ppm):").pack(pady=10)
        e = ttk.Entry(w)
        e.insert(0, str(self.settings.get("alert_threshold", 0.5)))
        e.pack()
        def save():
            try:
                self.settings["alert_threshold"] = float(e.get())
                self.save_settings()
                w.destroy()
                messagebox.showinfo("Éxito", "Ajustes guardados.")
            except:
                messagebox.showerror("Error", "Valor inválido.")
        ttk.Button(w, text="Guardar", command=save).pack(pady=20)

    # ———————————————————————————————————————————————————————————————————————
    # PESTAÑAS
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
        self._make_quick_help(f, "Selecciona un archivo .pssession exportado de PSTrace para analizarlo y guardarlo en la base de datos.")
        ttk.Button(f, text="Seleccionar Archivo .pssession", command=self.load_file).pack(pady=40)
        self.log_text = tk.Text(f, height=10, bg="#1e272e", fg="#2ecc71", font=("Courier", 10))
        self.log_text.pack(fill="x", padx=20)

    def build_query_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🔍 Consultas")
        self._make_quick_help(f, "Filtra y revisa sesiones previas guardadas en la base de datos por fecha, dispositivo o ID.")

        # Filtros
        filt = ttk.Frame(f, padding=10)
        filt.pack(fill="x")
        ttk.Label(filt, text="Fecha Inicio:").grid(row=0, column=0)
        self.date_start = DateEntry(filt, date_pattern="yyyy-mm-dd")
        self.date_start.grid(row=0, column=1, padx=5)
        ttk.Label(filt, text="Fecha Fin:").grid(row=0, column=2)
        self.date_end = DateEntry(filt, date_pattern="yyyy-mm-dd")
        self.date_end.grid(row=0, column=3, padx=5)

        ttk.Label(filt, text="Dispositivo:").grid(row=0, column=4)
        self.device_cmb = ttk.Combobox(filt, state="readonly")
        self.device_cmb.grid(row=0, column=5, padx=5)

        ttk.Button(filt, text="Buscar", command=self.query_sessions).grid(row=0, column=6, padx=10)

        # Tabla
        cols = ("ID", "Archivo", "Fecha", "Dispositivo", "Estado", "Nivel")
        self.tree = ttk.Treeview(f, columns=cols, show="headings", height=15)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=10, pady=10)
        self.tree.bind("<<TreeviewSelect>>", self.on_session_select)

        # Tags de colores
        self.tree.tag_configure("alert", background="#f8d7da", foreground="#721c24")
        self.tree.tag_configure("warning", background="#fff3cd", foreground="#856404")
        self.tree.tag_configure("safe", background="#d4edda", foreground="#155724")

    def build_detail_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📝 Detalles")
        self._make_quick_help(f, "Visualización estructurada de los resultados de la muestra analizada.")

        # Reemplazo de Text plano por Treeview (Mejora visual solicitada)
        cols = ("Metadato / Metal", "Valor", "Unidad", "Estado")
        self.tree_detail = ttk.Treeview(f, columns=cols, show="headings")
        for c in cols:
            self.tree_detail.heading(c, text=c)
            self.tree_detail.column(c, anchor="center")
        self.tree_detail.pack(fill="both", expand=True, padx=20, pady=20)

        # Tags semáforo
        self.tree_detail.tag_configure("SEGURA", background="#d4edda", foreground="#155724")
        self.tree_detail.tag_configure("ANOMALA", background="#fff3cd", foreground="#856404")
        self.tree_detail.tag_configure("CONTAMINADA", background="#f8d7da", foreground="#721c24")

    def build_curve_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📊 Curvas")
        self._make_quick_help(f, "Gráfica de voltagramas de la sesión actual.")
        self.fig_curve, self.ax_curve = plt.subplots(figsize=(8, 4), facecolor=COLOR_BG)
        self.canvas_curve = FigureCanvasTkAgg(self.fig_curve, master=f)
        self.canvas_curve.get_tk_widget().pack(fill="both", expand=True)

    def build_pca_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📈 PCA")
        self._make_quick_help(f, "Análisis de Componentes Principales para la detección de anomalías.")
        self.fig_pca, self.ax_pca = plt.subplots(figsize=(8, 4), facecolor=COLOR_BG)
        self.canvas_pca = FigureCanvasTkAgg(self.fig_pca, master=f)
        self.canvas_pca.get_tk_widget().pack(fill="both", expand=True)

    def build_ppm_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🗂 ppm")
        self._make_quick_help(f, "Estimación de concentración de metales pesados detectados.")
        self.tree_ppm = ttk.Treeview(f, columns=("Metal", "ppm"), show="headings")
        self.tree_ppm.heading("Metal", text="Metal Detectado")
        self.tree_ppm.heading("ppm", text="Concentración (ppm)")
        self.tree_ppm.pack(fill="both", expand=True, padx=20, pady=20)

    def build_iot_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🌐 IoT")
        self._make_quick_help(f, "Envío de datos procesados a la nube o dispositivos remotos vía TCP/IP.")
        tk.Label(f, text="Módulo IoT Activo", font=("Arial", 12)).pack(pady=20)

    # ———————————————————————————————————————————————————————————————————————
    # LÓGICA DE NEGOCIO Y CONSULTAS
    # ———————————————————————————————————————————————————————————————————————
    def load_file(self):
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if not path: return
        try:
            from pstrace_session import extract_session_dict
            from db_persistence import guardar_sesion_y_mediciones

            self.log_message(f"Procesando: {os.path.basename(path)}...")
            data = extract_session_dict(path)
            if not data: raise Exception("No se extrajeron datos")

            sid = guardar_sesion_y_mediciones(data["session_info"], data["measurements"])
            self.session_info = data["session_info"]
            self.session_info["id"] = sid
            self.current_data = pd.DataFrame(data["measurements"])

            self.update_detail_view()
            self.show_curve()
            self.log_message(f"✅ Sesión guardada con ID: {sid}")
            messagebox.showinfo("Éxito", f"Archivo procesado. ID: {sid}")
        except Exception as e:
            self.log_message(f"❌ Error: {e}")
            messagebox.showerror("Error", str(e))

    def update_detail_view(self):
        """Mejora visual de la pestaña detalles."""
        self.tree_detail.delete(*self.tree_detail.get_children())
        si = self.session_info
        self.tree_detail.insert("", "end", values=("ID Sesión", si.get("id"), "-", "-"))
        self.tree_detail.insert("", "end", values=("Archivo", si.get("filename"), "-", "-"))

        for _, m in self.current_data.iterrows():
            canon = normalize_classification(m.get("clasificacion", "SEGURA"))
            estado = display_label_from_label(canon)
            nivel = m.get("contamination_level", 0)
            self.tree_detail.insert("", "end", values=("MUESTRA: " + m.get("title"), f"{nivel:.2f}%", "%", estado), tags=(canon,))

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
                # Corregir lógica de visualización basada en GROUP_MAP canónico
                if group == 2: tag, txt = "alert", "CONTAMINADA"
                elif group == 1: tag, txt = "warning", "ANOMALA"
                else: tag, txt = "safe", "SEGURA"

                self.tree.insert("", "end", values=(r[0], r[1], r[2], r[3], txt, f"{r[5]:.2f}%"), tags=(tag,))
        except Exception as e:
            messagebox.showerror("Error Consulta", str(e))

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
        # Aquí se cargaría la data histórica para graficar...
        self.log_message(f"Seleccionada sesión {sid}")

    def show_curve(self):
        if self.current_data is None: return
        self.ax_curve.clear()
        self.ax_curve.set_facecolor(COLOR_BG)
        # Lógica de graficado simplificada...
        self.ax_curve.set_title("Voltagramas de la Sesión", color="white")
        self.canvas_curve.draw()

    def log_message(self, msg):
        self.log_text.insert("end", f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}
")
        self.log_text.see("end")


if __name__ == "__main__":
    app = Aplicacion()
    app.mainloop()
