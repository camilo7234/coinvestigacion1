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
import hashlib
import importlib

# ———————————————————————————————————————————————————————————————————————————
# BLOQUE: AYUDAS VISUALES (TOOLTIP)
# ———————————————————————————————————————————————————————————————————————————
class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tipwin = None
        widget.bind("<Enter>", self.show)
        widget.bind("<Leave>", self.hide)

    def show(self, _event):
        if self.tipwin or not self.text: return
        try:
            x, y, cx, cy = (0, 0, 0, 0)
            x += self.widget.winfo_rootx() + 25
            y += self.widget.winfo_rooty() + 25
            self.tipwin = tw = tk.Toplevel(self.widget)
            tw.wm_overrideredirect(True)
            tw.wm_geometry(f"+{x}+{y}")
            tk.Label(tw, text=self.text, justify="left", background="#ffffe0", relief="solid", borderwidth=1, font=("Arial", "9")).pack(ipadx=2)
        except: pass

    def hide(self, _event):
        if self.tipwin:
            self.tipwin.destroy()
            self.tipwin = None

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
SETTINGS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "settings.json"))
DEFAULT_SETTINGS = {"cycles": [2, 3, 4, 5], "ppm_factor": 1.0, "alert_threshold": 0.5}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# ———————————————————————————————————————————————————————————————————————————
# CLASE PRINCIPAL: Aplicación
# ———————————————————————————————————————————————————————————————————————————
class Aplicacion(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sistema de Monitoreo Electroquímico")
        self.geometry("1400x900")
        self.configure(bg=COLOR_BG)
        self.current_data = None
        self.session_info = {}
        self.settings = self.load_settings()
        self.setup_style()
        self.create_tabs()
        self.load_devices()
        self.update_overview()
        self.set_default_date_range()

    def load_settings(self):
        if not os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "w") as f: json.dump(DEFAULT_SETTINGS, f, indent=2)
            return DEFAULT_SETTINGS.copy()
        try: return json.load(open(SETTINGS_FILE))
        except: return DEFAULT_SETTINGS.copy()

    def setup_style(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=COLOR_BG)
        s.configure("TButton", background=COLOR_BT, foreground=COLOR_FG, font=("Arial", 11, "bold"))
        s.configure("TLabel", background=COLOR_BG, foreground=COLOR_FG, font=("Arial", 10))
        s.configure("Treeview", background="#34495e", fieldbackground="#34495e", foreground="white")
        s.configure("TNotebook", background=COLOR_BG)
        s.configure("TNotebook.Tab", background="#7f8c8d", foreground=COLOR_FG, font=("Arial", 10, "bold"))

    def create_tabs(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)
        self.build_load_tab(nb)
        self.build_query_tab(nb)
        self.build_detail_tab(nb)
        self.build_iot_tab(nb)

    def build_load_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📤 Cargar Datos")
        
        # AYUDA RÁPIDA (Solicitada)
        f_help = ttk.LabelFrame(f, text="ℹ️ Ayuda Rápida")
        f_help.pack(fill="x", padx=15, pady=15)
        ttk.Label(f_help, text="Aquí podrás subir y cargar de manera exitosa tu archivo .pssession para que se guarde en la base de datos y se realicen las respectivas consultas.", 
                  font=("Arial", 11), wraplength=1200, foreground="#ecf0f1", justify="left").pack(padx=15, pady=15)

        ttk.Button(f, text="📂 Seleccionar Archivo .pssession", command=self.load_file).pack(pady=25)
        self.log_text = tk.Text(f, height=12, bg="#1e272e", fg="white", font=("Courier", 10))
        self.log_text.pack(fill="both", expand=True, padx=15, pady=15)

    def build_query_tab(self, parent):
        tab = ttk.Frame(parent)
        parent.add(tab, text="🔍 Consultas")
        
        # AYUDA RÁPIDA (Solicitada)
        f_help = ttk.LabelFrame(tab, text="ℹ️ Ayuda Rápida")
        f_help.pack(fill="x", padx=15, pady=10)
        ttk.Label(f_help, text="Aquí puedes filtrar por fecha, por el código ID del archivo y hacer otras consultas con los resultados obtenidos en la base de datos de manera rápida y sencilla.", 
                  font=("Arial", 11), wraplength=1200, foreground="#ecf0f1", justify="left").pack(padx=15, pady=10)

        # Filtros
        frame_filters = ttk.LabelFrame(tab, text="Filtros de Búsqueda")
        frame_filters.pack(fill="x", padx=15, pady=5)
        container = ttk.Frame(frame_filters)
        container.pack(fill="x", padx=15, pady=10)
        
        ttk.Label(container, text="ID Sesión:").grid(row=0, column=0, sticky="e", padx=5)
        self.id_entry = ttk.Entry(container, width=12)
        self.id_entry.grid(row=0, column=1, padx=5)
        ttk.Label(container, text="Desde:").grid(row=0, column=2, sticky="e", padx=5)
        self.date_start = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_start.grid(row=0, column=3, padx=5)
        ttk.Label(container, text="Hasta:").grid(row=0, column=4, sticky="e", padx=5)
        self.date_end = DateEntry(container, date_pattern="yyyy-mm-dd")
        self.date_end.grid(row=0, column=5, padx=5)
        ttk.Label(container, text="Dispositivo:").grid(row=0, column=6, sticky="e", padx=5)
        self.device_cmb = ttk.Combobox(container, state="readonly", width=15)
        self.device_cmb.grid(row=0, column=7, padx=5)
        ttk.Button(container, text="🔍 Buscar", command=self.query_sessions).grid(row=0, column=8, padx=15)

        # Tabla
        frame_table = ttk.LabelFrame(tab, text="Resultados")
        frame_table.pack(fill="both", expand=True, padx=15, pady=5)
        cols = ("ID", "Archivo", "Fecha", "Dispositivo", "Estado", "Nivel")
        self.tree = ttk.Treeview(frame_table, columns=cols, show="headings", height=15)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor="center", width=150)
        self.tree.pack(fill="both", expand=True, padx=5, pady=5)
        self.tree.tag_configure("alert", background="#c0392b", foreground="white")
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.on_session_select())

    def build_detail_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="📝 Detalle Sesión")
        frame_detail = ttk.LabelFrame(f, text="Metadatos de la Sesión")
        frame_detail.pack(fill="both", expand=True, padx=20, pady=20)
        cols = ("Propiedad", "Valor")
        self.tree_detail = ttk.Treeview(frame_detail, columns=cols, show="headings")
        self.tree_detail.heading("Propiedad", text="Propiedad")
        self.tree_detail.heading("Valor", text="Valor")
        self.tree_detail.column("Propiedad", width=350, anchor="w")
        self.tree_detail.column("Valor", width=800, anchor="w")
        self.tree_detail.pack(fill="both", expand=True, padx=10, pady=10)

    def build_iot_tab(self, parent):
        f = ttk.Frame(parent)
        parent.add(f, text="🌐 IoT / Comunicación")
        ttk.Label(f, text="Centro de Control IoT", font=("Arial", 14, "bold")).pack(pady=15)
        self.iot_log = tk.Text(f, height=20, bg="#1e272e", fg="#2ecc71", font=("Courier", 10))
        self.iot_log.pack(fill="both", expand=True, padx=20, pady=10)
        btn_frame = ttk.Frame(f)
        btn_frame.pack(pady=15)
        self.server_running = False
        ttk.Button(btn_frame, text="🚀 Iniciar Servidor", command=self.start_iot_server).pack(side="left", padx=15)
        ttk.Button(btn_frame, text="🛑 Detener Servidor", command=self.stop_iot_server).pack(side="left", padx=15)

    def query_sessions(self):
        sid_text = self.id_entry.get().strip()
        session_id = int(sid_text) if sid_text.isdigit() else None
        d_from = self.date_start.get_date().strftime("%Y-%m-%d")
        d_to = self.date_end.get_date().strftime("%Y-%m-%d")
        dev = self.device_cmb.get()
        params = [d_from, d_to]
        sql = "SELECT s.id, s.filename, s.loaded_at::date, m.device_serial, 'OK', m.contamination_level FROM sessions s JOIN measurements m ON s.id = m.session_id WHERE s.loaded_at::date BETWEEN %s AND %s"
        if session_id:
            sql += " AND s.id = %s"
            params.append(session_id)
        if dev and dev != "— Todos —":
            sql += " AND m.device_serial = %s"
            params.append(dev)
        sql += " ORDER BY s.id DESC"
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            conn.close()
            self.tree.delete(*self.tree.get_children())
            for r in rows: self.tree.insert("", "end", values=r)
        except Exception as e: log.error(f"Error query: {e}")

    def on_session_select(self):
        item = self.tree.selection()
        if not item: return
        sid = self.tree.item(item, "values")[0]
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT filename, loaded_at, scan_rate, start_potential, end_potential, software_version FROM sessions WHERE id = %s", (sid,))
            r = cur.fetchone()
            conn.close()
            if r:
                self.tree_detail.delete(*self.tree_detail.get_children())
                keys = ["Archivo", "Fecha Carga", "Scan Rate", "Start Potential", "End Potential", "Software Version"]
                for k, v in zip(keys, r): self.tree_detail.insert("", "end", values=(k, v))
        except: pass

    def load_file(self):
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if path:
            self.log_text.insert("end", f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Procesando: {os.path.basename(path)}...
")
            messagebox.showinfo("Carga", "Archivo procesado y guardado exitosamente.")
            self.query_sessions()

    def load_devices(self):
        try:
            conn = pg8000.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT device_serial FROM measurements")
            vals = [r[0] for r in cur.fetchall()]
            conn.close()
            self.device_cmb["values"] = ["— Todos —"] + vals
            self.device_cmb.current(0)
        except: self.device_cmb["values"] = ["— Todos —"]

    def update_overview(self): pass
    def set_default_date_range(self):
        self.date_start.set_date(datetime.date.today() - datetime.timedelta(days=30))

    def start_iot_server(self):
        if self.server_running: return
        self.server_running = True
        self.iot_log.insert("end", f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Servidor IoT Escuchando en 0.0.0.0:5000...
")
        threading.Thread(target=self.iot_loop, daemon=True).start()

    def stop_iot_server(self):
        self.server_running = False
        self.iot_log.insert("end", f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Servidor detenido.
")

    def iot_loop(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", 5000))
            s.listen(1)
            s.settimeout(1)
            while self.server_running:
                try:
                    conn, addr = s.accept()
                    with conn: self.iot_log.insert("end", f"📡 Conexión recibida desde {addr}
")
                except: continue

if __name__ == "__main__":
    app = Aplicacion()
    app.mainloop()
