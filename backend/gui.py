from pathlib import Path
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from datetime import datetime
import ctypes
import getpass
import multiprocessing
import platform
import os
import subprocess
import sys
import threading
import time
import tkinter as tk



class ShimmieToolsGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Shimmie Tools GUI")
        self.geometry("1280x720")
        # configure colors
        colors = {'background_color': '#1e1e2c', 'text_color': '#E8E8EC', 'window_color': '#1e1e2c', 'window_text_color': '#E8E8EC'}
        self.configure(bg=colors['window_color'])
        # Set the style for ttk widgets
        style = ttk.Style()
        style.theme_use('clam')  # 'clam' theme allows more customization
        style.configure("TButton", background=colors['background_color'], foreground=colors['text_color'])
        style.configure("TFrame", background=colors['background_color'], foreground=colors['text_color'])
        style.configure("TLabel", background=colors['background_color'], foreground=colors['text_color'])
        style.configure("TEntry", background=colors['background_color'], foreground="#000000")
        style.configure("TCombobox", background=colors['background_color'], foreground=colors['text_color'])
        style.configure("Custom.TCheckbutton",     background=colors['background_color'], foreground=colors['text_color'], indicatorbackground=colors['background_color'], indicatorcolor=colors['text_color'], selectcolor=colors['background_color'])
        style.map('Custom.TCheckbutton', background=[('active', colors['window_color'])], foreground=[('disabled', 'gray')], indicatorbackground=[('selected', colors['text_color']), ('!selected', colors['background_color'])])
        # Create a main frame with the background color
        main_frame = tk.Frame(self, bg=colors['background_color'])
        main_frame.pack(fill=tk.BOTH, expand=True)
        self.label = tk.Label(self, text="Welcome to Shimmie Tools!", bg=colors['background_color'], fg=colors['text_color'])
        self.label.pack(pady=20)
        self._resizing = False
        self._resize_timer = None
        self._log_queue = []
        self._log_lock = threading.Lock()
        self.active_proc = None
        self.abort_button = ttk.Button(self, text="❌ Abort", command=self._abort_process, state="disabled")
        self.abort_button.pack(pady=5)


        self._create_widgets(main_frame, colors)

    def _create_widgets(self, parent, colors):
        self.bind("<Configure>", self._on_configure_event)

        # Configure styles for ttk widgets
        style = ttk.Style()
        style.configure("TNotebook", background=colors['background_color'], bordercolor=colors['background_color'])
        style.configure("TNotebook.Tab", background=colors['background_color'], foreground=colors['text_color'], padding=[10, 5])
        style.map("TNotebook.Tab", background=[("selected", colors['window_color'])], foreground=[("selected", colors['window_text_color'])])

        # Root layout: vertical paned window
        paned = tk.PanedWindow(self, orient=tk.VERTICAL)
        paned.pack(fill='both', expand=True)

        # Frame for notebook + progress bar
        top_frame = ttk.Frame(paned, style="TFrame")
        top_frame.columnconfigure(0, weight=1)
        top_frame.rowconfigure(0, weight=1)
        top_frame.rowconfigure(1, weight=0)

        # Notebook
        self.notebook = ttk.Notebook(top_frame, style="TNotebook")
        self.notebook.grid(row=0, column=0, sticky='nsew')

        self.booru_tab = self._create_booru_tab(self.notebook)
        self.precache_tab = self._create_precache_tab(self.notebook)
        self.wiki_tab = self._create_wiki_tab(self.notebook)


        # Progress bar
        self.progress = ttk.Progressbar(top_frame, mode='indeterminate')
        self.progress.grid(row=1, column=0, sticky='ew', padx=5, pady=(5, 0))

        # Add top_frame to the paned window
        paned.add(top_frame, minsize=300)

        # Bottom log section
        bottom_frame = ttk.Frame(paned)
        bottom_frame.columnconfigure(0, weight=1)
        bottom_frame.rowconfigure(0, weight=1)

        # Set the background color for the bottom frame
        #bottom_frame.configure(bg=colors['background_color'])

        self.log_output = ScrolledText(bottom_frame, height=10, state='disabled', wrap='word', bg="#1e1e1e", fg="#d4d4d4")
        self.log_output.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)

        # Add log to paned window
        paned.add(bottom_frame, minsize=100)

        # Style tags for ScrolledText
        self.log_output.tag_config("ncurses", foreground="#00cccc")
        self.log_output.tag_config("error", foreground="#ff5555")
        self.log_output.tag_config("bold", font=("Courier", 10, "bold"))

        self.bind_all("<Control-a>", self._select_all)

    def _create_booru_tab(self, notebook):
        import multiprocessing
        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Tag Images")

        # Allow column 1 to expand for entries
        frame.columnconfigure(1, weight=1)

        self.booru_args = {}
        row = 0

        # Helper: Add labeled entry + optional Browse
        def add_entry(label, key, default="", browse_func=None):
            nonlocal row
            self.booru_args[key] = tk.StringVar(value=default)
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky='w')
            container = ttk.Frame(frame)
            ttk.Entry(container, textvariable=self.booru_args[key], width=50).pack(side='left', fill='x', expand=True)
            if browse_func:
                ttk.Button(container, text="Browse", command=browse_func).pack(side='right')
            container.grid(row=row, column=1, sticky='ew', padx=2, pady=2)
            row += 1

        # Folder/Image input
        add_entry("Input Folder/Image", "images", "", lambda: self._select_path_for("images", folder=True))

        # Cache input
        default_cache = str(Path("backend/database/posts_cache.db").resolve())
        add_entry("Input Cache File", "cache", default_cache, lambda: self._select_path_for("cache", filetypes=[("SQLite DB", "*.db")]))

        # Batch size
        self.booru_args["batch"] = tk.IntVar(value=10)
        ttk.Label(frame, text="Batch Size").grid(row=row, column=0, sticky='w')
        ttk.Spinbox(frame, from_=1, to=99, textvariable=self.booru_args["batch"], width=5).grid(row=row, column=1, sticky='w')
        row += 1

        # Model selector
        self.booru_args["model"] = tk.StringVar(value="vit-large")
        ttk.Label(frame, text="Model").grid(row=row, column=0, sticky='w')
        model_options = ["vit", "vit-large", "swinv2", "convnext"]
        ttk.Combobox(frame, textvariable=self.booru_args["model"], values=model_options, state="readonly", style="TEntry").grid(row=row, column=1, sticky='w')
        row += 1

        # Thresholds
        for label, key, default in [("General Threshold", "gt", 0.50),
                                    ("Rating Threshold", "rt", 0.35),
                                    ("Character Threshold", "ct", 0.30)]:
            self.booru_args[key] = tk.DoubleVar(value=default)
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky='w')
            ttk.Spinbox(frame, from_=0.0, to=1.0, increment=0.01,
                        textvariable=self.booru_args[key], format="%.2f", width=6).grid(row=row, column=1, sticky='w')
            row += 1

        # Threads
        max_threads = multiprocessing.cpu_count()
        step = 2 if max_threads <= 24 else 4
        thread_options = list(range(step, max_threads + 1, step))
        default_threads = max_threads if max_threads <= 24 else 24
        self.booru_args["threads"] = tk.IntVar(value=default_threads)
        ttk.Label(frame, text="Threads").grid(row=row, column=0, sticky='w')
        ttk.Combobox(frame, textvariable=self.booru_args["threads"],
                    values=thread_options, state="readonly", width=5, style="TEntry").grid(row=row, column=1, sticky='w')
        row += 1

        # Checkboxes
        for label, key, default in [("Include Subfolders", "subfolder", False)]:
            self.booru_args[key] = tk.BooleanVar(value=default)
            ttk.Checkbutton(frame, text=label, variable=self.booru_args[key], style="Custom.TCheckbutton").grid(row=row, columnspan=2, sticky='w')
            row += 1

        # Run button
        ttk.Button(frame, text="Run Tagger", command=self.run_booru).grid(row=row, column=0, columnspan=2, pady=10)

        return frame

    def _create_precache_tab(self, notebook):
        import multiprocessing

        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Precache Posts")
        frame.columnconfigure(1, weight=1)

        self.precache_args = {}

        default_input = str(Path("input/posts.json").resolve())
        default_output = str(Path("tools/posts_cache.db").resolve())

        def add_entry(label, key, default, browse_func=None):
            nonlocal row
            self.precache_args[key] = tk.StringVar(value=default)
            ttk.Label(frame, text=label, style="TLabel").grid(row=row, column=0, sticky='w')
            container = ttk.Frame(frame)
            ttk.Entry(container, textvariable=self.precache_args[key], width=50, style="TEntry").pack(side='left', fill='x', expand=True)
            if browse_func:
                ttk.Button(container, text="Browse", command=browse_func).pack(side='right')
            container.grid(row=row, column=1, sticky='ew', padx=2, pady=2)
            row += 1

        row = 0
        add_entry("posts.json path", "posts_json", default_input,
          lambda: self._select_path_for("posts_json", filetypes=[("JSON files", "*.json")]))
        add_entry("Output DB Path", "output", default_output,
          lambda: self._select_path_for("output", filetypes=[("SQLite DB", "*.db")], save=True, default_ext=".db"))

        max_threads = multiprocessing.cpu_count()
        step = 2 if max_threads <= 24 else 4
        thread_options = list(range(step, max_threads + 1, step))
        default_threads = max_threads if max_threads <= 24 else 24
        self.precache_args["threads"] = tk.IntVar(value=default_threads)

        ttk.Label(frame, text="Threads").grid(row=row, column=0, sticky='w')
        ttk.Combobox(frame, textvariable=self.precache_args["threads"],
                    values=thread_options, state="readonly", width=5, style="TEntry").grid(row=row, column=1, sticky='w')
        row += 1

        ttk.Button(frame, text="Run Precache", command=self.run_precache).grid(row=row, column=0, columnspan=2, pady=10)

        return frame

    def _create_wiki_tab(self, notebook):

        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Import Wikis")
        frame.columnconfigure(1, weight=1)

        default_user = getpass.getuser()
        default_password = None
        default_db = "shimmiedb"

        self.wiki_args = {
            "user": tk.StringVar(value=default_user),
            "dbl": tk.StringVar(value=default_password),
            "db": tk.StringVar(value=default_db),
            "start_page": tk.IntVar(value=1),
            "pages": tk.IntVar(value=200),
            "convert": tk.StringVar(value="shimmie"),
            "update_existing": tk.BooleanVar(),
            "update_cache": tk.BooleanVar(),
            "clear_cache": tk.BooleanVar()
        }

        row = 0

        def add_text_entry(label, var, hide=False):
            nonlocal row
            ttk.Label(frame, text=label, style="TLabel").grid(row=row, column=0, sticky='w')
            ttk.Entry(frame, textvariable=var, width=50, show='•' if hide else '', style="TEntry").grid(row=row, column=1, sticky='ew', padx=2, pady=2)
            row += 1

        add_text_entry("Database User", self.wiki_args["user"])
        add_text_entry("Database Password", self.wiki_args["dbl"], hide=True)
        add_text_entry("Database Name", self.wiki_args["db"])
        add_text_entry("Start Page", self.wiki_args["start_page"])
        add_text_entry("Page Count", self.wiki_args["pages"])

        ttk.Label(frame, text="Convert Mode").grid(row=row, column=0, sticky='w')
        convert_modes = ["raw", "markdown", "html", "shimmie"]
        ttk.Combobox(frame, textvariable=self.wiki_args["convert"],
                    values=convert_modes, state="readonly", width=48, style="TEntry").grid(row=row, column=1, sticky='w')
        row += 1

        for label, key in [("Update Existing", "update_existing"),
                        ("Update Cache", "update_cache"),
                        ("Clear Cache", "clear_cache")]:
            ttk.Checkbutton(frame, text=label, variable=self.wiki_args[key], style="Custom.TCheckbutton").grid(row=row, columnspan=2, sticky='w')
            row += 1

        ttk.Button(frame, text="Import Wikis", command=self.run_wiki, style="TButton").grid(row=row, column=0, columnspan=2, pady=10)

        return frame

    def run_booru(self):
        # Sanity check for image input
        img_path = self.booru_args["images"].get().strip()
        if not img_path or img_path.upper() == "NO_INPUT":
            messagebox.showerror("Missing Input", "Please select an image or folder to process.")
            return
        args = ["python", "-u", "scripts/booru_csv_maker.py"]
        for key, entry in self.booru_args.items():
            if isinstance(entry, tk.BooleanVar):
                if entry.get():
                    args.append(f"--{key}")
            elif isinstance(entry, tk.StringVar) or isinstance(entry, tk.IntVar) or isinstance(entry, tk.DoubleVar):
                val = entry.get()
                if val not in ("", None):
                    args.append(f"--{key}={val}")
        lines = [
            f"Input:        {img_path}",
            f"Batch Size:   {self.booru_args['batch'].get()}",
            f"Model:        {self.booru_args['model'].get()}",
            f"Gen Thresh:   {self.booru_args['gt'].get():.2f}",
            f"Rating:       {self.booru_args['rt'].get():.2f}",
            f"Char Thresh:  {self.booru_args['ct'].get():.2f}",
            f"Threads:      {self.booru_args['threads'].get()}",
            f"Input Cache:  {self.booru_args['cache'].get()}"
        ]
        self.render_summary("Tagger Run Summary", lines)
        self._run_script(args)

    def run_precache(self):
        args = ["python", "-u", "scripts/precache_posts_sqlite.py"]
        args.append(self.precache_args["posts_json"].get())
        args += ["-o", self.precache_args["output"].get(),
                "--threads", str(self.precache_args["threads"].get())]

        lines = [
            f"Input File:   {self.precache_args['posts_json'].get()}",
            f"Output DB:    {self.precache_args['output'].get()}",
            f"Threads:      {self.precache_args['threads'].get()}",
        ]
        self.render_summary("Precache Run Summary", lines)
        self._run_script(args)

    def run_wiki(self):
        args = ["python", "-u", "scripts/import_danbooru_wikis.py"]
        for key, val in self.wiki_args.items():
            if isinstance(val, tk.BooleanVar):
                if val.get():
                    args.append(f"--{key.replace('_', '-')}")
            else:
                args.append(f"--{key.replace('_', '-')}={val.get()}")

        lines = [
            f"Database:     {self.wiki_args['db'].get()}",
            f"User:         {self.wiki_args['user'].get()}",
            f"Start Page:   {self.wiki_args['start_page'].get()}",
            f"Page Count:   {self.wiki_args['pages'].get()}",
            f"Convert Mode: {self.wiki_args['convert'].get()}",
            f"Update Cache: {'Yes' if self.wiki_args['update_cache'].get() else 'No'}",
            f"Clear Cache:  {'Yes' if self.wiki_args['clear_cache'].get() else 'No'}",
            f"Update Exist: {'Yes' if self.wiki_args['update_existing'].get() else 'No'}",
        ]
        self.render_summary("Wiki Import Summary", lines)
        self._run_script(args)

    def _run_script(self, cmd):
        def run():
            self.abort_button.config(state="normal")
            self.progress.start(10)
            self.log(f"$ {' '.join(cmd)}\n")

            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"  # 👈 Unbuffered output

            # Choose the correct venv path
            venv_base = Path(__file__).resolve().parent / "sd_tag_editor" / "venv"
            venv_py = venv_base / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

            if venv_py.exists() and Path(cmd[0]).name.startswith("python"):
                cmd[0] = str(venv_py)
            else:
                self.log("[WARN] Virtual environment not found. Falling back to system Python.")

            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    cwd="backend",
                    env=env
                )
                self.abort_button.config(state="normal")  # Enable abort
                self.active_proc = proc  # Save to self for aborting

                def enqueue_output():
                    for line in iter(proc.stdout.readline, ''):
                        if line:
                            self.after(0, self.log, line.rstrip())

                reader_thread = threading.Thread(target=enqueue_output, daemon=True)
                reader_thread.start()

                # Wait in background (non-blocking main thread)
                while proc.poll() is None:
                    time.sleep(0.1)

            except Exception as e:
                self.after(0, self.log, f"[ERROR] {e}")
            finally:
                self.after(0, self._on_script_complete)

        threading.Thread(target=run, daemon=True).start()

    def _on_script_complete(self):
        self.progress.stop()
        self.abort_button.config(state="disabled")
        self.active_proc = None

    def _abort_process(self):
        if self.active_proc and self.active_proc.poll() is None:
            # Optional: add check for safe-to-abort here
            try:
                self.active_proc.terminate()
                self.log("⚠️ Process aborted by user.")
            except Exception as e:
                self.log(f"[ERROR] Failed to abort: {e}")
            finally:
                self.abort_button.config(state="disabled")
                self.progress.stop()

    def _select_path_for(self, key: str, filetypes=None, folder=False, save=False, default_ext=None):
        if not hasattr(self, "_last_dir"):
            self._last_dir = str(Path.home())  # fallback to home directory

        path = None

        if folder:
            path = filedialog.askdirectory(
                title="Select Folder",
                initialdir=self._last_dir
            )
        elif save:
            path = filedialog.asksaveasfilename(
                title="Save File",
                defaultextension=default_ext,
                filetypes=filetypes or [("All files", "*.*")],
                initialdir=self._last_dir
            )
        else:
            path = filedialog.askopenfilename(
                title="Select File",
                filetypes=filetypes or [("All files", "*.*")],
                initialdir=self._last_dir
            )

        if path:
            # Store last-used directory
            self._last_dir = str(Path(path).parent)

            # Append extension if saving and missing
            if save and default_ext:
                ext = default_ext if default_ext.startswith(".") else f".{default_ext}"
                if not path.lower().endswith(ext):
                    path += ext

            if key in self.booru_args:
                self.booru_args[key].set(path.strip())
            elif key in self.precache_args:
                self.precache_args[key].set(path.strip())
            elif key in self.wiki_args:
                self.wiki_args[key].set(path.strip())

    def _select_all(self, event):
        widget = event.widget
        if isinstance(widget, (tk.Entry, ttk.Entry)):
            widget.selection_range(0, 'end')
            return "break"

    def render_summary(self, title: str, lines: list[str], tag="ncurses"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_title = f"{title}  •  {timestamp}"
        width = max(len(line) for line in lines + [full_title]) + 4

        border = "┌" + "─" * (width - 2) + "┐"
        divider = "├" + "─" * (width - 2) + "┤"
        bottom = "└" + "─" * (width - 2) + "┘"

        content = [
            border,
            f"│ {full_title.center(width - 4)} │",
            divider,
            *[f"│ {line.ljust(width - 4)} │" for line in lines],
            bottom,
            "",  # extra newline
            "⌛ Starting script...\n"
        ]

        for line in content:
            self._append_log(line, tag=tag)

    def log(self, text):
        self._append_log(text)

    def _should_autoscroll(self):
        # Returns True if the log is scrolled to bottom
        return self.log_output.yview()[1] == 1.0


    def _append_log(self, text, tag=None):
        self.log_output.configure(state='normal')
        is_at_bottom = self._should_autoscroll()

        self.log_output.insert(tk.END, text + '\n', tag or ())

        if is_at_bottom:
            self.log_output.see(tk.END)

        self.log_output.configure(state='disabled')

    def _flush_log_queue(self):
        with self._log_lock:
            while self._log_queue:
                self._append_log(self._log_queue.pop(0))

    def _on_configure_event(self, event):
        self._resizing = True
        if self._resize_timer:
            self.after_cancel(self._resize_timer)
        self._resize_timer = self.after(500, self._on_resize_done)

    def _on_resize_done(self):
        self._resizing = False
        self._flush_log_queue()

if __name__ == "__main__":
    app = ShimmieToolsGUI()

    class StdoutRedirector:
        def __init__(self, log_func):
            self.log_func = log_func
        def write(self, text):
            if text.strip():
                self.log_func(text.strip())
        def flush(self):
            pass

    sys.stdout = StdoutRedirector(lambda msg: app.log(msg))
    sys.stderr = sys.stdout

    app.mainloop()
