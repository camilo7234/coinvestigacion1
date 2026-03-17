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

_base_dir = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(os.path.dirname(_base_dir), "settings.json")
DEFAULT_SETTINGS = {"cycles": [2, 3, 4, 5], "ppm_factor": 1.0, "alert_threshold": 0.5}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# ———————————————————————————————————————————————————————————————————————————
# CLASES AUXILIARES
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
        x, y, _, _ = self.widget.bbox("insert") if hasattr(self.widget, "bbox") and self.widget.winfo_class() in ("Text", "Entry") else (0,0,0,0)
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 25
        self.tipwin = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        tk.Label(tw, text=self.text, justify="left", background="#ffffe0", relief="solid", borderwidth=1, font=("Arial", "8")).pack(ipadx=1)

    def hide(self, _event):
        if self.tipwin: self.tipwin.destroy()
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
        self.server_running = False
        self.server_thread = None
        self.setup_style()
        self.create_menu()
        self.create_tabs()
        self.load_devices()

    def load_settings(self):
        if not os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "w") as f: json.dump(DEFAULT_SETTINGS, f, indent=2)
            except: pass
            return DEFAULT_SETTINGS.copy()
        try: return json.load(open(SETTINGS_FILE))
        except: return DEFAULT_SETTINGS.copy()

    def save_settings(self):
        try:
            with open(SETTINGS_FILE, "w") as f: json.dump(self.settings, f, indent=2)
        except Exception as e: log.error(f"Error guardando settings: {e}")

    def _make_quick_help(self, parent, text):
        help_frame = tk.LabelFrame(parent, text="ℹ️ AYUDA RÁPIDA", font=("Arial", 10, "bold"), bg="#1a252f", fg="#3498db", labelanchor="nw", padx=10, pady=8)
        help_frame.pack(fill="x", side="top", padx=15, pady=10)
        lbl = tk.Label(help_frame, text=text, font=("Arial", 9, "italic"), bg="#1a252f", fg="#bdc3c7", justify="left", wraplength=1200)
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
        m = tk.Menu(self); f = tk.Menu(m, tearoff=0)
        f.add_command(label="Ajustes", command=self.show_settings)
        f.add_separator(); f.add_command(label="Salir", command=self.quit)
        m.add_cascade(label="Archivo", menu=f); self.config(menu=m)

    def show_settings(self):
        w = tk.Toplevel(self); w.title("Ajustes del Sistema"); w.geometry("350x250"); w.configure(bg="#ecf0f1")
        ttk.Label(w, text="Umbral de Alerta (ppm):", background="#ecf0f1", foreground="#2c3e50").pack(pady=(20, 5))
        e = ttk.Entry(w, justify="center"); e.insert(0, str(self.settings.get("alert_threshold", 0.5))); e.pack(pady=5)
        def save():
            try: self.settings["alert_threshold"] = float(e.get()); self.save_settings(); w.destroy(); messagebox.showinfo("Éxito", "Configuración actualizada.")
            except: messagebox.showerror("Error", "Ingresa un número válido.")
        ttk.Button(w, text="Guardar Cambios", command=save).pack(pady=30)

    def create_tabs(self):
        nb = ttk.Notebook(self); nb.pack(fill="both", expand=True)
        self.build_load_tab(nb); self.build_query_tab(nb); self.build_detail_tab(nb); self.build_curve_tab(nb); self.build_pca_tab(nb); self.build_ppm_tab(nb); self.build_iot_tab(nb)

    def build_load_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="📤 Cargar Datos")
        self._make_quick_help(f, "Aquí podrás subir y cargar de manera exitosa tu archivo .pssession para ser procesado.")
        container = ttk.Frame(f, padding=40); container.pack()
        ttk.Button(container, text="📂 Seleccionar Archivo .pssession", command=self.load_file).pack(pady=20)
        self.log_text = tk.Text(f, height=12, bg="#1e272e", fg="#2ecc71", font=("Courier", 10), padx=10, pady=10)
        self.log_text.pack(fill="x", padx=25, pady=(5, 20))

    def build_query_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="🔍 Consultas")
        self._make_quick_help(f, "Aquí puedes filtrar por fecha, por el código ID del archivo y hacer otras consultas.")
        filt = ttk.LabelFrame(f, text="Filtros de Búsqueda", padding=15); filt.pack(fill="x", padx=15, pady=5)
        ttk.Label(filt, text="ID:").grid(row=0, column=0)
        self.id_entry = ttk.Entry(filt, width=8); self.id_entry.grid(row=0, column=1, padx=5)
        self.date_start = DateEntry(filt, date_pattern="yyyy-mm-dd", width=12); self.date_start.grid(row=0, column=3, padx=5)
        self.date_end = DateEntry(filt, date_pattern="yyyy-mm-dd", width=12); self.date_end.grid(row=0, column=5, padx=5)
        self.device_cmb = ttk.Combobox(filt, state="readonly", width=15); self.device_cmb.grid(row=0, column=7, padx=5)
        ttk.Button(filt, text="🔎 Ejecutar Consulta", command=self.query_sessions).grid(row=0, column=8, padx=20)
        cols = ("ID", "Archivo", "Fecha", "Sensor", "Estado de Muestra", "Contaminación (%)")
        self.tree = ttk.Treeview(f, columns=cols, show="headings", height=18)
        for c in cols: self.tree.heading(c, text=c); self.tree.column(c, anchor="center", width=150)
        self.tree.pack(fill="both", expand=True, padx=15, pady=15); self.tree.bind("<<TreeviewSelect>>", self.on_session_select)
        self.tree.tag_configure("alert", background="#f8d7da", foreground="#721c24"); self.tree.tag_configure("warning", background="#fff3cd", foreground="#856404"); self.tree.tag_configure("safe", background="#d4edda", foreground="#155724")

    def build_detail_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="📝 Detalle Sesión")
        self._make_quick_help(f, "Esta pestaña presenta los datos técnicos y resultados de clasificación de manera estructurada.")
        top = ttk.LabelFrame(f, text="Info General", padding=15); top.pack(fill="x", padx=15, pady=5)
        self.lbl_session_id = ttk.Label(top, text="ID: ---"); self.lbl_session_id.grid(row=0, column=0, padx=20)
        self.lbl_session_file = ttk.Label(top, text="Archivo: ---"); self.lbl_session_file.grid(row=0, column=1, padx=20)
        bot = ttk.LabelFrame(f, text="Resultados", padding=15); bot.pack(fill="both", expand=True, padx=15, pady=10)
        self.tree_detail = ttk.Treeview(bot, columns=("Muestra", "Valor", "Unidad", "Estado"), show="headings")
        for c in ("Muestra", "Valor", "Unidad", "Estado"): self.tree_detail.heading(c, text=c); self.tree_detail.column(c, anchor="center")
        self.tree_detail.pack(fill="both", expand=True)

    def build_curve_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="📊 Curvas")
        ctrls = ttk.Frame(f, padding=10); ctrls.pack(fill="x")
        self.cmb_curve = ttk.Combobox(ctrls, state="readonly", width=30); self.cmb_curve.pack(side="left"); self.cmb_curve.bind("<<ComboboxSelected>>", lambda e: self.show_curve())
        self.fig_curve, self.ax_curve = plt.subplots(figsize=(10, 5), facecolor=COLOR_BG); self.ax_curve.set_facecolor("#1e272e")
        self.canvas_curve = FigureCanvasTkAgg(self.fig_curve, master=f); self.canvas_curve.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=15)

    def build_pca_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="📈 PCA")
        self.fig_pca, self.ax_pca = plt.subplots(figsize=(10, 5), facecolor=COLOR_BG); self.ax_pca.set_facecolor("#1e272e")
        self.canvas_pca = FigureCanvasTkAgg(self.fig_pca, master=f); self.canvas_pca.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=15)

    def build_ppm_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="🗂 ppm")
        self.tree_ppm = ttk.Treeview(f, columns=("Metal", "Concentración", "Límite Máximo", "Exceso"), show="headings")
        for c in ("Metal", "Concentración", "Límite Máximo", "Exceso"): self.tree_ppm.heading(c, text=c); self.tree_ppm.column(c, anchor="center")
        self.tree_ppm.pack(fill="both", expand=True, padx=20, pady=20)

    def build_iot_tab(self, parent):
        f = ttk.Frame(parent); parent.add(f, text="🌐 IoT / Comunicación")
        self._make_quick_help(f, "Módulo de control del servidor IoT, conexión remota y envío de archivos.")
        
        frame_conf = ttk.LabelFrame(f, text="Configuración"); frame_conf.pack(fill="x", padx=15, pady=5)
        ttk.Label(frame_conf, text="IP:").grid(row=0, column=0, padx=5)
        self.iot_ip_var = tk.StringVar(value=""); ttk.Entry(frame_conf, textvariable=self.iot_ip_var, width=15).grid(row=0, column=1, padx=5)
        ttk.Label(frame_conf, text="Puerto:").grid(row=0, column=2, padx=5)
        self.iot_port_var = tk.IntVar(value=5000); ttk.Entry(frame_conf, textvariable=self.iot_port_var, width=8).grid(row=0, column=3, padx=5)
        
        frame_ctrls = ttk.Frame(f); frame_ctrls.pack(fill="x", padx=15, pady=10)
        ttk.Button(frame_ctrls, text="🚀 Iniciar Servidor", command=self.start_iot_server).pack(side="left", padx=5)
        ttk.Button(frame_ctrls, text="🛑 Detener Servidor", command=self.stop_iot_server).pack(side="left", padx=5)
        ttk.Button(frame_ctrls, text="🔌 Probar Conexión", command=self.test_iot_connection).pack(side="left", padx=5)
        ttk.Button(frame_ctrls, text="📤 Enviar Archivo", command=self.send_iot_file).pack(side="left", padx=5)
        
        self.iot_progress = ttk.Progressbar(f, length=500, mode="determinate"); self.iot_progress.pack(pady=10)
        self.iot_log = tk.Text(f, height=10, bg="#1e272e", fg="white", font=("Courier", 9)); self.iot_log.pack(fill="x", padx=15, pady=5)

    def log_iot(self, msg): self.iot_log.insert("end", f"{msg}
"); self.iot_log.see("end")

    def start_iot_server(self):
        if self.server_running: return
        def server_loop():
            host, port = "0.0.0.0", self.iot_port_var.get()
            dest_dir = os.path.join(os.path.dirname(__file__), "..", "archivos_recibidos")
            os.makedirs(dest_dir, exist_ok=True)
            self.log_iot(f"🌐 Servidor escuchando en {port}..."); self.server_running = True
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, port)); s.listen(5); s.settimeout(1)
                while self.server_running:
                    try:
                        conn, addr = s.accept()
                        with conn:
                            self.log_iot(f"📡 Conexión de {addr}")
                            data = conn.recv(1024).decode()
                            if data.startswith("{"):
                                header = json.loads(data)
                                if header.get("action") == "ping": conn.sendall(b"PONG
")
                                elif header.get("action") == "send_file":
                                    conn.sendall(b"ACK")
                                    with open(os.path.join(dest_dir, header["filename"]), "wb") as f:
                                        rec = 0
                                        while rec < header["size"]:
                                            chunk = conn.recv(4096)
                                            if not chunk: break
                                            f.write(chunk); rec += len(chunk)
                                    self.log_iot(f"✅ Recibido: {header['filename']}")
                    except socket.timeout: continue
                    except Exception as e: self.log_iot(f"❌ Error: {e}")
            self.server_running = False; self.log_iot("🛑 Servidor detenido.")
        self.server_thread = threading.Thread(target=server_loop, daemon=True); self.server_thread.start()

    def stop_iot_server(self): self.server_running = False

    def test_iot_connection(self):
        try:
            with socket.create_connection((self.iot_ip_var.get(), self.iot_port_var.get()), timeout=3) as s:
                s.sendall(json.dumps({"action": "ping"}).encode() + b"
")
                if s.recv(128): messagebox.showinfo("Éxito", "Conexión exitosa.")
        except Exception as e: messagebox.showerror("Error", str(e))

    def send_iot_file(self):
        path = filedialog.askopenfilename()
        if not path: return
        try:
            size, fname = os.path.getsize(path), os.path.basename(path)
            with socket.create_connection((self.iot_ip_var.get(), self.iot_port_var.get())) as s:
                s.sendall(json.dumps({"action": "send_file", "filename": fname, "size": size}).encode() + b"
")
                if s.recv(8) == b"ACK":
                    with open(path, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""): s.sendall(chunk)
                    messagebox.showinfo("Éxito", "Archivo enviado.")
        except Exception as e: messagebox.showerror("Error", str(e))

    def load_file(self):
        path = filedialog.askopenfilename(filetypes=[("PSSession", "*.pssession")])
        if not path: return
        try:
            from pstrace_session import extract_session_dict
            from db_persistence import guardar_sesion_y_mediciones
            data = extract_session_dict(path)
            sid = guardar_sesion_y_mediciones(data["session_info"], data["measurements"])
            self.session_info = data["session_info"]; self.session_info["id"] = sid
            self.current_data = pd.DataFrame(data["measurements"])
            self.update_detail_view(); self.update_curve_selector(); self.show_curve(); self.query_sessions()
            messagebox.showinfo("Éxito", f"Cargado ID: {sid}")
        except Exception as e: log.error(f"Error: {e}"); messagebox.showerror("Error", str(e))

    def update_detail_view(self):
        self.tree_detail.delete(*self.tree_detail.get_children())
        self.lbl_session_id.config(text=f"ID: {self.session_info.get('id', 'N/A')}")
        self.lbl_session_file.config(text=f"Archivo: {self.session_info.get('filename', 'N/A')}")
        if self.current_data is not None:
            for _, m in self.current_data.iterrows():
                canon = normalize_classification(m.get("clasificacion", "SEGURA"))
                self.tree_detail.insert("", "end", values=(m.get("title"), f"{m.get('contamination_level', 0):.2f}", "%", display_label_from_label(canon)))

    def update_curve_selector(self):
        if self.current_data is not None:
            self.cmb_curve["values"] = [f"{i}: {r['title']}" for i, r in self.current_data.iterrows()]
            if len(self.current_data) > 0: self.cmb_curve.current(0)

    def query_sessions(self):
        try:
            conn = pg8000.connect(**DB_CONFIG); cur = conn.cursor()
            cur.execute("SELECT s.id, s.filename, s.loaded_at::date, m.device_serial, m.classification_group, m.contamination_level FROM sessions s JOIN measurements m ON s.id = m.session_id ORDER BY s.id DESC")
            rows = cur.fetchall(); conn.close(); self.tree.delete(*self.tree.get_children())
            for r in rows:
                g = int(r[4]); tag = "alert" if g==2 else "warning" if g==1 else "safe"
                self.tree.insert("", "end", values=(r[0], r[1], r[2], r[3], label_from_group(g), f"{r[5]:.2f}%"), tags=(tag,))
        except Exception as e: log.error(f"Error: {e}")

    def load_devices(self):
        try:
            conn = pg8000.connect(**DB_CONFIG); cur = conn.cursor(); cur.execute("SELECT DISTINCT device_serial FROM measurements WHERE device_serial IS NOT NULL"); devs = [r[0] for r in cur.fetchall()]; conn.close()
            self.device_cmb["values"] = ["TODOS"] + devs; self.device_cmb.current(0)
        except: pass

    def on_session_select(self, _=None):
        sel = self.tree.selection()
        if not sel: return
        sid = self.tree.item(sel[0])["values"][0]
        try:
            conn = pg8000.connect(**DB_CONFIG); cur = conn.cursor()
            cur.execute("SELECT filename, scan_rate, start_potential, end_potential, software_version FROM sessions WHERE id = %s", (sid,))
            s = cur.fetchone()
            if s: self.session_info = {"id": sid, "filename": s[0], "scan_rate": s[1], "start_potential": s[2], "end_potential": s[3], "software_version": s[4]}
            cur.execute("SELECT title, device_serial, curve_count, classification_group, contamination_level, pca_scores FROM measurements WHERE session_id = %s", (sid,))
            m_list = [{"title": r[0], "device_serial": r[1], "curve_count": r[2], "classification_group": r[3], "clasificacion": label_from_group(r[3]), "contamination_level": r[4], "pca_scores": r[5]} for r in cur.fetchall()]
            self.current_data = pd.DataFrame(m_list); conn.close(); self.update_detail_view(); self.update_curve_selector(); self.show_curve(); self.show_pca()
        except Exception as e: log.error(f"Error: {e}")

    def show_curve(self):
        if self.current_data is None or self.cmb_curve.get() == "": return
        try:
            d = self.current_data.iloc[self.cmb_curve.current()]; s = d.get("pca_scores")
            if s: self.ax_curve.clear(); self.ax_curve.set_facecolor("#1e272e"); self.ax_curve.plot(s, color="#3498db"); self.ax_curve.set_title(f"Voltagrama: {d['title']}", color="white"); self.canvas_curve.draw()
        except: pass

    def show_pca(self):
        if self.current_data is None: return
        self.ax_pca.clear(); self.ax_pca.set_facecolor("#1e272e")
        if len(self.current_data) > 1: self.ax_pca.bar(range(len(self.current_data)), self.current_data["contamination_level"].tolist(), color="#e74c3c")
        self.canvas_pca.draw()

    def log_message(self, msg): self.log_text.insert("end", f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}
"); self.log_text.see("end")

if __name__ == "__main__":
    app = Aplicacion(); app.mainloop()
