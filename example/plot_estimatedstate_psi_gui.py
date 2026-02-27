import gzip
import inspect
import logging
import math
import os
import shutil
import sys
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

NUMERIC_IMC_TYPES = {
    'int8_t',
    'uint8_t',
    'int16_t',
    'uint16_t',
    'int32_t',
    'uint32_t',
    'int64_t',
    'fp32_t',
    'fp64_t',
}

def _ensure_import_paths():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    generated_from_cwd = os.path.join(os.getcwd(), 'pyimc_generated', '__init__.py')
    generated_from_project = os.path.join(project_root, 'pyimc_generated', '__init__.py')

    # pyimclsts.network resolves pyimc_generated from current working directory.
    if not os.path.isfile(generated_from_cwd) and os.path.isfile(generated_from_project):
        os.chdir(project_root)


def _resolve_log_path(path: str) -> str:
    if not os.path.isfile(path):
        raise FileNotFoundError(f'Log file not found: {path}')
    if path.endswith('.lsf') or path.endswith('.lsf.gz'):
        return path
    raise ValueError('Input file must end in .lsf or .lsf.gz')


def _decompress_if_needed(path: str):
    if not path.endswith('.gz'):
        return path, None

    lsf_path = path[:-3]  # strip ".gz"
    src_stat = os.stat(path)
    if os.path.isfile(lsf_path):
        lsf_stat = os.stat(lsf_path)
        if lsf_stat.st_mtime_ns >= src_stat.st_mtime_ns:
            return lsf_path, None

    tmp_lsf_path = f'{lsf_path}.tmp'
    with gzip.open(path, 'rb') as src, open(tmp_lsf_path, 'wb') as dst:
        shutil.copyfileobj(src, dst)

    os.replace(tmp_lsf_path, lsf_path)
    return lsf_path, None


def _curve_label_from_log_path(log_path: str) -> str:
    parent_dir = os.path.basename(os.path.dirname(os.path.abspath(log_path)))
    if parent_dir:
        return parent_dir
    return os.path.basename(log_path)


class PsiPlotApp:
    def __init__(self, tk_module, filedialog_module, messagebox_module, ttk_module, canvas_cls, root, n_module, pg_module, figure, ax):
        self.tk = tk_module
        self.filedialog = filedialog_module
        self.messagebox = messagebox_module
        self.ttk = ttk_module
        self.canvas_cls = canvas_cls
        self.root = root
        self.n = n_module
        self.pg = pg_module
        self.figure = figure
        self.canvas = None
        self.ax = ax
        self._pan_active = False
        self._pan_start = None

        self.log_paths = []
        self.workers_var = self.tk.IntVar(value=max(1, min(4, os.cpu_count() or 1)))
        self.show_debug_var = self.tk.BooleanVar(value=False)
        self.selected_message_var = self.tk.StringVar()
        self.selected_field_var = self.tk.StringVar()
        self.status_var = self.tk.StringVar(value='Add one or more logs or folders, then click Load + Plot.')
        self.progress_var = self.tk.DoubleVar(value=0.0)
        self.progress_text_var = self.tk.StringVar(value='Progress: 0/0 (0.0%)')
        self.debug_enabled = True
        self.settings_window = None
        self.message_combo = None
        self.show_debug_var.trace_add('write', self._on_show_debug_changed)
        self.message_classes = {}
        self.message_fields = {}
        self._build_message_field_catalog()

        self._build_ui()

    def _build_message_field_catalog(self):
        message_names = list(getattr(self.pg.messages, '_message_ids', {}).values())
        for message_name in message_names:
            message_cls = getattr(self.pg.messages, message_name, None)
            if not inspect.isclass(message_cls):
                continue
            numeric_fields = self._get_numeric_fields(message_cls)
            if numeric_fields:
                self.message_classes[message_name] = message_cls
                self.message_fields[message_name] = numeric_fields

        if not self.message_classes:
            raise RuntimeError('No messages with numeric fields were found in IMC bindings.')

        default_msg = 'EstimatedState' if 'EstimatedState' in self.message_classes else next(iter(self.message_classes))
        default_fields = self.message_fields[default_msg]
        default_field = 'psi' if default_msg == 'EstimatedState' and 'psi' in default_fields else default_fields[0]
        self.selected_message_var.set(default_msg)
        self.selected_field_var.set(default_field)

    def _get_numeric_fields(self, message_cls):
        try:
            message_obj = message_cls()
        except Exception:
            return []

        numeric_fields = []
        for field_name in getattr(message_obj.Attributes, 'fields', []):
            field_descriptor = getattr(type(message_obj), field_name, None)
            field_def = getattr(field_descriptor, '_field_def', {})
            field_type = field_def.get('type')
            if field_type in NUMERIC_IMC_TYPES:
                numeric_fields.append(field_name)
        return numeric_fields

    def _build_ui(self):
        self.root.title('IMC Field Plotter')
        self.root.geometry('1200x700')
        self._build_menu()

        container = self.ttk.Frame(self.root, padding=10)
        container.pack(fill=self.tk.BOTH, expand=True)

        top_content = self.ttk.Frame(container)
        top_content.pack(side=self.tk.TOP, fill=self.tk.BOTH, expand=True)

        left_panel = self.ttk.Frame(top_content, width=320)
        left_panel.pack(side=self.tk.LEFT, fill=self.tk.Y, padx=(0, 10))
        left_panel.pack_propagate(False)

        right_panel = self.ttk.Frame(top_content)
        right_panel.pack(side=self.tk.RIGHT, fill=self.tk.BOTH, expand=True)

        controls = self.ttk.Frame(left_panel)
        controls.pack(fill=self.tk.X)

        browse_btn = self.ttk.Button(controls, text='Add Logs...', command=self._on_browse)
        browse_btn.pack(side=self.tk.LEFT, fill=self.tk.X, expand=True, padx=(0, 6))

        browse_folder_btn = self.ttk.Button(controls, text='Add Folder...', command=self._on_browse_folder)
        browse_folder_btn.pack(side=self.tk.LEFT, fill=self.tk.X, expand=True)

        remove_btn = self.ttk.Button(left_panel, text='Remove Selected', command=self._remove_selected)
        remove_btn.pack(fill=self.tk.X, pady=(8, 0))

        clear_btn = self.ttk.Button(left_panel, text='Clear All', command=self._clear_logs)
        clear_btn.pack(fill=self.tk.X, pady=(6, 0))

        list_frame = self.ttk.Frame(left_panel)
        list_frame.pack(fill=self.tk.BOTH, expand=True, pady=(8, 0))

        y_scrollbar = self.ttk.Scrollbar(list_frame, orient=self.tk.VERTICAL)
        x_scrollbar = self.ttk.Scrollbar(list_frame, orient=self.tk.HORIZONTAL)
        self.log_listbox = self.tk.Listbox(
            list_frame,
            selectmode=self.tk.EXTENDED,
            yscrollcommand=y_scrollbar.set,
            xscrollcommand=x_scrollbar.set,
        )
        y_scrollbar.config(command=self.log_listbox.yview)
        x_scrollbar.config(command=self.log_listbox.xview)
        list_frame.grid_rowconfigure(0, weight=1)
        list_frame.grid_columnconfigure(0, weight=1)
        self.log_listbox.grid(row=0, column=0, sticky='nsew')
        y_scrollbar.grid(row=0, column=1, sticky='ns')
        x_scrollbar.grid(row=1, column=0, columnspan=2, sticky='ew')

        self.load_btn = self.ttk.Button(left_panel, text='Load + Plot', command=self._on_load)
        self.load_btn.pack(fill=self.tk.X, pady=(8, 0))

        self.progress_bar = self.ttk.Progressbar(
            left_panel,
            orient=self.tk.HORIZONTAL,
            mode='determinate',
            variable=self.progress_var,
            maximum=100.0,
        )
        self.progress_bar.pack(fill=self.tk.X, pady=(8, 0))

        progress_label = self.ttk.Label(left_panel, textvariable=self.progress_text_var)
        progress_label.pack(fill=self.tk.X, pady=(4, 0))

        status_label = self.ttk.Label(left_panel, textvariable=self.status_var, wraplength=300)
        status_label.pack(fill=self.tk.X, pady=(10, 0))

        self.debug_frame = self.ttk.LabelFrame(container, text='Debug Log')
        debug_scroll = self.ttk.Scrollbar(self.debug_frame, orient=self.tk.VERTICAL)
        self.debug_text = self.tk.Text(self.debug_frame, height=8, wrap='word', yscrollcommand=debug_scroll.set)
        debug_scroll.config(command=self.debug_text.yview)
        self.debug_text.pack(side=self.tk.LEFT, fill=self.tk.BOTH, expand=True)
        debug_scroll.pack(side=self.tk.RIGHT, fill=self.tk.Y)
        self.debug_text.insert(self.tk.END, 'Debug initialized.\n')
        self.debug_text.configure(state=self.tk.DISABLED)

        # Create canvas directly under right_panel to avoid parent/layout issues.
        self.canvas = self.canvas_cls(self.figure, master=right_panel)
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.pack(fill=self.tk.BOTH, expand=True)

        self._toggle_debug_visibility()
        self._connect_mouse_interactions()
        self._reset_plot()

    def _build_menu(self):
        menubar = self.tk.Menu(self.root)
        settings_menu = self.tk.Menu(menubar, tearoff=0)
        settings_menu.add_command(label='General Settings', command=self._open_general_settings)
        menubar.add_cascade(label='Settings', menu=settings_menu)
        self.root.config(menu=menubar)

    def _open_general_settings(self):
        if self.settings_window is not None and self.settings_window.winfo_exists():
            self.settings_window.lift()
            self.settings_window.focus_force()
            return

        self.settings_window = self.tk.Toplevel(self.root)
        self.settings_window.title('Settings')
        self.settings_window.geometry('420x320')
        self.settings_window.transient(self.root)

        notebook = self.ttk.Notebook(self.settings_window)
        notebook.pack(fill=self.tk.BOTH, expand=True, padx=10, pady=10)

        general_tab = self.ttk.Frame(notebook, padding=10)
        notebook.add(general_tab, text='General Settings')

        workers_frame = self.ttk.Frame(general_tab)
        workers_frame.pack(fill=self.tk.X, pady=(0, 12))
        workers_label = self.ttk.Label(workers_frame, text='Parallel workers:')
        workers_label.pack(side=self.tk.LEFT)
        workers_spin = self.ttk.Spinbox(
            workers_frame,
            from_=1,
            to=max(1, os.cpu_count() or 1),
            textvariable=self.workers_var,
            width=6,
        )
        workers_spin.pack(side=self.tk.RIGHT)

        debug_check = self.ttk.Checkbutton(
            general_tab,
            text='Show Debug Panel',
            variable=self.show_debug_var,
            command=self._toggle_debug_visibility,
        )
        debug_check.pack(anchor=self.tk.W)

        message_frame = self.ttk.Frame(general_tab)
        message_frame.pack(fill=self.tk.X, pady=(12, 6))
        message_label = self.ttk.Label(message_frame, text='Message:')
        message_label.pack(side=self.tk.LEFT)
        self.message_combo = self.ttk.Combobox(
            message_frame,
            textvariable=self.selected_message_var,
            values=sorted(self.message_classes.keys()),
            state='readonly',
            width=28,
        )
        self.message_combo.pack(side=self.tk.RIGHT, fill=self.tk.X, expand=True)
        self.message_combo.bind('<<ComboboxSelected>>', self._on_message_selected)
        self.message_combo.bind('<KeyPress>', self._on_message_keypress)

        field_frame = self.ttk.Frame(general_tab)
        field_frame.pack(fill=self.tk.X, pady=(0, 8))
        field_label = self.ttk.Label(field_frame, text='Field:')
        field_label.pack(side=self.tk.LEFT)
        self.field_combo = self.ttk.Combobox(
            field_frame,
            textvariable=self.selected_field_var,
            state='readonly',
            width=28,
        )
        self.field_combo.pack(side=self.tk.RIGHT, fill=self.tk.X, expand=True)
        self._update_field_combo_values(preferred=self.selected_field_var.get())

        close_btn = self.ttk.Button(general_tab, text='Close', command=self.settings_window.destroy)
        close_btn.pack(anchor=self.tk.E, pady=(16, 0))

    def _on_message_selected(self, _event=None):
        self._update_field_combo_values()
        self._debug(
            f'Plot selection changed to {self.selected_message_var.get()}.{self.selected_field_var.get()}'
        )

    def _on_message_keypress(self, event):
        char = (event.char or '').lower()
        if not char.isalpha():
            return None

        values = sorted(self.message_classes.keys())
        for message_name in values:
            if message_name.lower().startswith(char):
                self.selected_message_var.set(message_name)
                self._on_message_selected()
                return 'break'
        return None

    def _update_field_combo_values(self, preferred=None):
        message_name = self.selected_message_var.get()
        fields = self.message_fields.get(message_name, [])
        if not fields:
            self.selected_field_var.set('')
            if hasattr(self, 'field_combo'):
                self.field_combo.configure(values=[])
            return

        selected = preferred if preferred in fields else self.selected_field_var.get()
        if selected not in fields:
            selected = fields[0]
        self.selected_field_var.set(selected)
        if hasattr(self, 'field_combo'):
            self.field_combo.configure(values=fields)

    def _on_show_debug_changed(self, *_):
        self._toggle_debug_visibility()

    def _toggle_debug_visibility(self):
        if self.show_debug_var.get():
            self.debug_frame.pack(side=self.tk.BOTTOM, fill=self.tk.X, pady=(10, 0))
        else:
            self.debug_frame.pack_forget()

    def _connect_mouse_interactions(self):
        self.canvas.mpl_connect('scroll_event', self._on_scroll_zoom)
        self.canvas.mpl_connect('button_press_event', self._on_button_press)
        self.canvas.mpl_connect('button_release_event', self._on_button_release)
        self.canvas.mpl_connect('motion_notify_event', self._on_mouse_move)

    def _on_scroll_zoom(self, event):
        if event.inaxes != self.ax or event.xdata is None or event.ydata is None:
            return

        scale = 1.2
        if event.button == 'up':
            scale_factor = 1.0 / scale
        elif event.button == 'down':
            scale_factor = scale
        else:
            return

        cur_xlim = self.ax.get_xlim()
        cur_ylim = self.ax.get_ylim()
        xdata = event.xdata
        ydata = event.ydata

        x_left = xdata - cur_xlim[0]
        x_right = cur_xlim[1] - xdata
        y_bottom = ydata - cur_ylim[0]
        y_top = cur_ylim[1] - ydata

        self.ax.set_xlim(xdata - x_left * scale_factor, xdata + x_right * scale_factor)
        self.ax.set_ylim(ydata - y_bottom * scale_factor, ydata + y_top * scale_factor)
        self.canvas.draw_idle()

    def _on_button_press(self, event):
        if event.inaxes != self.ax:
            return
        # Middle mouse button press starts panning.
        if event.button == 2 and event.x is not None and event.y is not None:
            x0, x1 = self.ax.get_xlim()
            y0, y1 = self.ax.get_ylim()
            bbox = self.ax.bbox
            # Use pixel-space movement to avoid jitter from changing data transforms.
            x_scale = (x1 - x0) / max(bbox.width, 1.0)
            y_scale = (y1 - y0) / max(bbox.height, 1.0)
            self._pan_active = True
            self._pan_start = {
                'x_pixel': event.x,
                'y_pixel': event.y,
                'xlim': (x0, x1),
                'ylim': (y0, y1),
                'x_scale': x_scale,
                'y_scale': y_scale,
            }

    def _on_button_release(self, event):
        if event.button == 2:
            self._pan_active = False
            self._pan_start = None

    def _on_mouse_move(self, event):
        if not self._pan_active or event.inaxes != self.ax:
            return
        if event.x is None or event.y is None or self._pan_start is None:
            return

        dx_pixels = event.x - self._pan_start['x_pixel']
        dy_pixels = event.y - self._pan_start['y_pixel']
        dx = dx_pixels * self._pan_start['x_scale']
        dy = dy_pixels * self._pan_start['y_scale']

        x0, x1 = self._pan_start['xlim']
        y0, y1 = self._pan_start['ylim']
        self.ax.set_xlim(x0 - dx, x1 - dx)
        self.ax.set_ylim(y0 - dy, y1 - dy)
        self.canvas.draw_idle()

    def _reset_plot(self):
        self.ax.clear()
        message_name = self.selected_message_var.get()
        field_name = self.selected_field_var.get()
        self.ax.set_title(f'{message_name}.{field_name} over Relative Time')
        self.ax.set_xlabel('Relative time [s]')
        self.ax.set_ylabel(field_name)
        self.ax.grid(True, linestyle='--', alpha=0.4)
        self.canvas.draw()

    def _set_status(self, message):
        self.status_var.set(message)

    def _set_progress(self, completed, total):
        if total <= 0:
            pct = 0.0
        else:
            pct = (completed / total) * 100.0
        self.progress_var.set(pct)
        self.progress_text_var.set(f'Progress: {completed}/{total} ({pct:.1f}%)')

    def _debug(self, message):
        if not self.debug_enabled:
            return
        logging.info(message)
        self.debug_text.configure(state=self.tk.NORMAL)
        self.debug_text.insert(self.tk.END, f'{message}\n')
        self.debug_text.see(self.tk.END)
        self.debug_text.configure(state=self.tk.DISABLED)

    def _on_browse(self):
        selected_paths = self.filedialog.askopenfilenames(
            title='Select IMC log files',
            filetypes=[('IMC log files', '*.lsf *.lsf.gz'), ('All files', '*.*')],
        )
        if not selected_paths:
            self._debug('No files selected.')
            return

        added, ignored_invalid, ignored_existing = self._add_log_paths(selected_paths)
        self.status_var.set(
            f'Added {added} log(s), skipped {ignored_invalid} invalid and {ignored_existing} existing. '
            f'Total selected: {len(self.log_paths)}.'
        )
        self._debug(
            f'File selection added {added} logs; ignored {ignored_invalid} invalid and '
            f'{ignored_existing} already selected.'
        )

    def _on_browse_folder(self):
        selected_folder = self.filedialog.askdirectory(
            title='Select a folder to search recursively for Data.lsf / Data.lsf.gz'
        )
        if not selected_folder:
            self._debug('No folder selected.')
            return

        discovered_paths = self._discover_data_logs(selected_folder)
        if not discovered_paths:
            self.status_var.set('No Data.lsf or Data.lsf.gz files were found in the selected folder.')
            self._debug(f'No logs discovered under {selected_folder}.')
            return

        added, ignored_invalid, ignored_existing = self._add_log_paths(discovered_paths)
        self.status_var.set(
            f'Discovered {len(discovered_paths)} log(s), added {added}, '
            f'skipped {ignored_invalid} invalid and {ignored_existing} existing. '
            f'Total selected: {len(self.log_paths)}.'
        )
        self._debug(
            f'Folder selection discovered {len(discovered_paths)} logs under {selected_folder}; '
            f'added {added}, ignored {ignored_invalid} invalid and {ignored_existing} already selected.'
        )

    def _discover_data_logs(self, root_folder):
        discovered = []
        for dirpath, _dirnames, filenames in os.walk(root_folder):
            has_data_lsf = 'Data.lsf' in filenames
            has_data_lsf_gz = 'Data.lsf.gz' in filenames
            if has_data_lsf:
                discovered.append(os.path.join(dirpath, 'Data.lsf'))
            elif has_data_lsf_gz:
                discovered.append(os.path.join(dirpath, 'Data.lsf.gz'))
        discovered.sort()
        return discovered

    def _add_log_paths(self, paths):
        added = 0
        ignored_invalid = 0
        ignored_existing = 0
        for path in paths:
            try:
                valid_path = _resolve_log_path(path)
            except Exception:
                ignored_invalid += 1
                self._debug(f'Ignored invalid path: {path}')
                continue
            if valid_path in self.log_paths:
                ignored_existing += 1
                continue
            self.log_paths.append(valid_path)
            added += 1

        self._refresh_log_list()
        return added, ignored_invalid, ignored_existing

    def _refresh_log_list(self):
        self.log_listbox.delete(0, self.tk.END)
        for path in self.log_paths:
            self.log_listbox.insert(self.tk.END, path)

    def _remove_selected(self):
        selected_indices = list(self.log_listbox.curselection())
        if not selected_indices:
            return
        for idx in reversed(selected_indices):
            del self.log_paths[idx]
        self._refresh_log_list()
        self.status_var.set(f'Selected logs: {len(self.log_paths)}.')
        self._debug(f'Removed selected logs. Remaining: {len(self.log_paths)}.')

    def _clear_logs(self):
        self.log_paths = []
        self._refresh_log_list()
        self.status_var.set('Log list cleared.')
        self._debug('Cleared all logs.')

    def _on_load(self):
        if not self.log_paths:
            self.messagebox.showerror('Missing files', 'Please add one or more logs or folders.')
            self._debug('Load aborted: no logs selected.')
            return

        message_name = self.selected_message_var.get()
        field_name = self.selected_field_var.get()
        message_cls = self.message_classes.get(message_name)
        if message_cls is None or not field_name:
            self.messagebox.showerror('Missing selection', 'Please choose a message and field in Settings.')
            self._debug('Load aborted: missing message/field selection.')
            return

        workers = max(1, int(self.workers_var.get()))
        self._set_progress(0, len(self.log_paths))
        self.load_btn.config(state=self.tk.DISABLED)
        self.status_var.set(
            f'Loading {len(self.log_paths)} log(s) with {workers} worker(s) and parsing {message_name}.{field_name}...'
        )
        self._debug(
            f'Start loading {len(self.log_paths)} log(s) with {workers} worker(s) for {message_name}.{field_name}.'
        )

        thread = threading.Thread(
            target=self._load_and_plot_worker,
            args=(list(self.log_paths), message_cls, message_name, field_name),
            daemon=True,
        )
        thread.start()

    def _parse_one_log(self, log_path, message_cls, field_name):
        input_lsf_path, temp_file_to_remove = _decompress_if_needed(log_path)
        timestamps = []
        field_values = []

        def on_message(msg, callback):
            timestamps.append(msg._header.timestamp)
            field_values.append(getattr(msg, field_name))

        try:
            sub = self.n.subscriber(self.n.file_interface(input=input_lsf_path), use_mp=False)
            sub.subscribe_async(on_message, msg_id=message_cls)
            sub.run()
        finally:
            if temp_file_to_remove and os.path.isfile(temp_file_to_remove):
                os.remove(temp_file_to_remove)

        if not field_values:
            return None

        t0 = timestamps[0]
        rel_times = [t - t0 for t in timestamps]
        return (_curve_label_from_log_path(log_path), rel_times, field_values, t0, timestamps[-1])

    def _load_and_plot_worker(self, selected_paths, message_cls, message_name, field_name):
        try:
            datasets = []
            total_logs = len(selected_paths)
            workers = max(1, min(int(self.workers_var.get()), total_logs))
            parsed = 0

            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_path = {}
                for log_path in selected_paths:
                    self.root.after(0, self._debug, f'Queueing {log_path}')
                    future_to_path[executor.submit(self._parse_one_log, log_path, message_cls, field_name)] = log_path

                for future in as_completed(future_to_path):
                    log_path = future_to_path[future]
                    parsed += 1
                    self.root.after(
                        0,
                        self._set_status,
                        f'Parsed {parsed}/{total_logs}: {os.path.basename(log_path)}',
                    )
                    self.root.after(0, self._set_progress, parsed, total_logs)
                    try:
                        result = future.result()
                    except Exception as exc:
                        self.root.after(0, self._debug, f'Failed {log_path}: {exc}')
                        continue

                    if result is None:
                        self.root.after(0, self._debug, f'No {message_name}.{field_name} samples found in {log_path}')
                        continue

                    label, rel_times, field_values, t0, t_end = result
                    datasets.append((label, rel_times, field_values))
                    self.root.after(
                        0,
                        self._debug,
                        f'Parsed {log_path}: {len(field_values)} samples, t0={t0:.3f}, t_end={t_end:.3f}',
                    )

            if not datasets:
                raise RuntimeError(f'No {message_name}.{field_name} samples were found in the selected logs.')

            self.root.after(0, self._set_progress, total_logs, total_logs)
            self.root.after(0, self._debug, f'Finished parsing. Plotting {len(datasets)} dataset(s).')
            self.root.after(0, self._render_plot, datasets, message_name, field_name)
        except Exception as exc:
            self.root.after(0, self._handle_error, str(exc))

    def _render_plot(self, datasets, message_name, field_name):
        self.ax.clear()
        total_points = 0
        label_counts = {}
        for label, rel_times, field_values in datasets:
            label_counts[label] = label_counts.get(label, 0) + 1
            plot_label = label if label_counts[label] == 1 else f'{label} ({label_counts[label]})'
            finite_points = [
                (x, y) for x, y in zip(rel_times, field_values)
                if isinstance(x, (int, float))
                and isinstance(y, (int, float))
                and math.isfinite(x)
                and math.isfinite(y)
            ]
            if not finite_points:
                self._debug(f'Skipped curve {label}: no finite points.')
                continue
            xvals, yvals = zip(*finite_points)
            self.ax.plot(xvals, yvals, linewidth=1.0, label=plot_label)
            total_points += len(xvals)
            self._debug(f'Rendered curve {plot_label}: {len(xvals)} finite points.')
        self.ax.set_title(f'{message_name}.{field_name} over Relative Time')
        self.ax.set_xlabel('Relative time [s]')
        self.ax.set_ylabel(field_name)
        self.ax.grid(True, linestyle='--', alpha=0.4)
        if len(datasets) > 1:
            self.ax.legend(loc='best')
        self.ax.relim()
        self.ax.autoscale_view()
        self.canvas.draw()
        self._debug('Canvas draw() called.')

        self.status_var.set(
            f'Plotted {len(datasets)} log(s), {total_points} {message_name}.{field_name} samples total.'
        )
        self.load_btn.config(state=self.tk.NORMAL)
        self._debug(f'Plot complete. Total points: {total_points}.')

    def _handle_error(self, message):
        self.status_var.set('Failed to load log.')
        self.load_btn.config(state=self.tk.NORMAL)
        self.messagebox.showerror('Error', message)
        self._debug(f'ERROR: {message}')


def main():
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
    _ensure_import_paths()
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except Exception as exc:
        raise RuntimeError(
            "tkinter is required for this GUI. On Ubuntu/Debian install it with: sudo apt install python3-tk"
        ) from exc

    try:
        import pyimclsts.network as n
        import pyimc_generated as pg
    except Exception as exc:
        raise RuntimeError(
            'Could not import pyimclsts/pyimc_generated. Generate bindings first '
            '(see pyimclsts/README.md) and run this script from the pyimclsts project context.'
        ) from exc

    try:
        warnings.filterwarnings(
            'ignore',
            message='Unable to import Axes3D.*',
            category=UserWarning,
            module='matplotlib.projections',
        )
        import matplotlib
        matplotlib.use('TkAgg', force=True)
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    except Exception as exc:
        raise RuntimeError(
            'matplotlib with Tk support is required. Install it with: pip3 install matplotlib'
        ) from exc

    root = tk.Tk()
    figure = Figure(figsize=(10, 5), dpi=100)
    ax = figure.add_subplot(111)
    PsiPlotApp(tk, filedialog, messagebox, ttk, FigureCanvasTkAgg, root, n, pg, figure, ax)
    root.mainloop()


if __name__ == '__main__':
    main()
