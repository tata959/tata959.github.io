import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
import numpy as np
import re
from datetime import datetime


class FinalCSVProcessor:
    def __init__(self, file_path=None):
        self.df = None
        self.original_df = None
        self.current_mask = None
        self.operation_history = []
        self.root = tk.Tk()
        self.root.title("终极CSV处理工具")
        self.root.geometry("1000x800")

        self.setup_ui()
        if file_path:
            self.load_file(file_path)
        else:
            self.ask_load_file()

    def setup_ui(self):
        # 顶部控制栏
        control_frame = ttk.Frame(self.root)
        control_frame.pack(fill='x', padx=10, pady=5)

        ttk.Button(control_frame, text="打开文件", command=self.ask_load_file).pack(side='left')
        ttk.Button(control_frame, text="撤销", command=self.undo_operation).pack(side='left', padx=5)
        ttk.Button(control_frame, text="重置", command=self.reset_all).pack(side='left')

        # 搜索框
        search_frame = ttk.Frame(control_frame)
        search_frame.pack(side='right', padx=10)
        ttk.Label(search_frame, text="搜索字段:").pack(side='left')
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=20)
        search_entry.pack(side='left')
        search_entry.bind('<KeyRelease>', self.filter_columns)

        self.export_btn = ttk.Button(control_frame, text="导出数据", command=self.export_data, state='disabled')
        self.export_btn.pack(side='right')

        # 主界面分为左右两栏
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True)

        # 左栏：字段操作面板
        left_panel = ttk.Frame(main_paned, width=600)
        main_paned.add(left_panel, weight=2)

        # 带滚动条的字段区域
        canvas = tk.Canvas(left_panel)
        scrollbar = ttk.Scrollbar(left_panel, orient="vertical", command=canvas.yview)
        self.scroll_frame = ttk.Frame(canvas)

        self.scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self.scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # 右栏：操作历史
        right_panel = ttk.Frame(main_paned, width=300)
        main_paned.add(right_panel, weight=1)

        history_frame = ttk.LabelFrame(right_panel, text="操作历史")
        history_frame.pack(fill="both", expand=True, padx=5, pady=5)

        self.history_list = tk.Listbox(history_frame, height=20, width=35)
        vsb = ttk.Scrollbar(history_frame, orient="vertical", command=self.history_list.yview)
        self.history_list.configure(yscrollcommand=vsb.set)

        self.history_list.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

    def filter_columns(self, event=None):
        search_term = self.search_var.get().lower()
        for child in self.scroll_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame):
                # 直接获取LabelFrame的text属性
                label_text = child["text"]
                # 提取字段名称（移除"字段: "前缀）
                col_name = label_text.split(": ")[1].strip() if ": " in label_text else label_text
                if search_term in col_name.lower():
                    child.grid()
                else:
                    child.grid_remove()

    def update_history_display(self):
        self.history_list.delete(0, tk.END)
        for op in reversed(self.operation_history):
            timestamp = op['timestamp'].strftime("%H:%M:%S")
            entry = f"[{timestamp}] {op['type']} - {op['description']}"
            self.history_list.insert(tk.END, entry)

    def ask_load_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if file_path:
            self.load_file(file_path)

    def load_file(self, file_path):
        try:
            # 修改后的文件读取方式
            self.df = pd.read_csv(file_path, low_memory=False)  # 添加low_memory=False
            self.original_df = self.df.copy()
            self.current_mask = pd.Series([True] * len(self.df))
            self.operation_history = []
            self.create_column_ui()
            self.export_btn['state'] = 'normal'
            self.update_history_display()
        except Exception as e:
            messagebox.showerror("错误", f"文件读取失败: {str(e)}")

    def detect_col_type(self, series):
        """改进后的类型检测方法，处理空白字段"""
        # 检查是否全部为空值
        if series.isna().all():
            return '空白型'

        # 尝试转换为数值型
        try:
            converted = pd.to_numeric(series, errors='coerce')
            # 如果原始数据有值但转换后出现NaN，说明是文本型
            if series.notna().any() and converted.isna().any():
                return '文本型'
            # 检查是否有浮点型特征
            if converted.dtype == float and (converted % 1 != 0).any():
                return '数值型（浮点）'
            return '数值型（整型）'
        except:
            return '文本型'

    def create_column_ui(self):
        # 清除旧UI组件
        for widget in self.scroll_frame.winfo_children():
            widget.destroy()

        # 创建新的字段UI
        for idx, col in enumerate(self.df.columns):
            col_frame = ttk.LabelFrame(self.scroll_frame, text=f"字段: {col}")
            col_frame.grid(row=idx, column=0, sticky="ew", padx=5, pady=2)

            # 类型显示和操作按钮
            col_type = self.detect_col_type(self.df[col])
            ttk.Label(col_frame, text=f"类型: {col_type}").pack(side='left', padx=5)

            btn_frame = ttk.Frame(col_frame)
            btn_frame.pack(side='right')

            # 为空白字段添加替换按钮
            if col_type == '空白型':
                ttk.Button(btn_frame, text="填充值",
                           command=lambda c=col: self.fill_empty_column(c)).pack(side='left', padx=2)
            elif col_type == '文本型':
                ttk.Button(btn_frame, text="替换",
                           command=lambda c=col: self.open_replace_window(c)).pack(side='left', padx=2)
                ttk.Button(btn_frame, text="筛选",
                           command=lambda c=col: self.open_filter_window(c)).pack(side='left', padx=2)
            else:
                ttk.Label(col_frame, text="数值型字段支持范围筛选").pack(side='right')
                ttk.Button(col_frame, text="数值筛选",
                           command=lambda c=col: self.open_number_filter(c)).pack(side='right', padx=2)

    def fill_empty_column(self, col):
        """为空白字段填充值的专用方法，支持关联修改"""
        fill_win = tk.Toplevel(self.root)
        fill_win.title(f"填充空白字段 - {col}")
        fill_win.geometry("600x500")

        # 记录原始数据状态
        original_data = self.df[col].copy()

        notebook = ttk.Notebook(fill_win)
        notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # 标签页1: 简单填充
        simple_frame = ttk.Frame(notebook)
        notebook.add(simple_frame, text="简单填充")

        ttk.Label(simple_frame, text=f"为字段 '{col}' 设置统一填充值:").pack(pady=5)
        self.simple_value = tk.StringVar()
        ttk.Entry(simple_frame, textvariable=self.simple_value).pack(pady=5)

        # 标签页2: 关联填充
        link_frame = ttk.Frame(notebook)
        notebook.add(link_frame, text="关联填充")

        # 关联字段选择
        link_field_frame = ttk.LabelFrame(link_frame, text="选择关联字段")
        link_field_frame.pack(fill='x', padx=5, pady=5)

        self.link_field_var = tk.StringVar()
        link_combobox = ttk.Combobox(link_field_frame, textvariable=self.link_field_var,
                                     state='readonly')
        link_combobox.pack(fill='x', padx=5, pady=5)

        # 填充规则设置
        rule_frame = ttk.LabelFrame(link_frame, text="设置填充规则")
        rule_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # 使用Treeview显示和编辑填充规则
        columns = ('link_value', 'fill_value')
        self.link_rules_tree = ttk.Treeview(rule_frame, columns=columns, show='headings', height=5)

        self.link_rules_tree.heading('link_value', text='关联字段值')
        self.link_rules_tree.heading('fill_value', text='填充值')

        vsb = ttk.Scrollbar(rule_frame, orient="vertical", command=self.link_rules_tree.yview)
        hsb = ttk.Scrollbar(rule_frame, orient="horizontal", command=self.link_rules_tree.xview)
        self.link_rules_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.link_rules_tree.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')
        hsb.pack(side='bottom', fill='x')

        # 添加/删除规则按钮
        rule_btn_frame = ttk.Frame(rule_frame)
        rule_btn_frame.pack(fill='x', pady=5)

        ttk.Button(rule_btn_frame, text="添加规则",
                   command=lambda: self.add_link_rule(col)).pack(side='left', padx=2)
        ttk.Button(rule_btn_frame, text="删除规则",
                   command=self.remove_link_rule).pack(side='left', padx=2)

        # 动态更新关联字段选项
        def update_link_fields():
            available_cols = [c for c in self.df.columns if c != col]
            link_combobox['values'] = available_cols
            if available_cols:
                self.link_field_var.set(available_cols[0])
                self.update_link_rules()

        # 当关联字段变化时更新规则列表
        def on_link_field_changed(*args):
            self.update_link_rules()

        self.link_field_var.trace_add('write', on_link_field_changed)
        update_link_fields()

        # 确认按钮
        btn_frame = ttk.Frame(fill_win)
        btn_frame.pack(fill='x', padx=10, pady=10)

        def execute_fill():
            try:
                # 获取当前选择的标签页
                current_tab = notebook.index(notebook.select())

                if current_tab == 0:  # 简单填充
                    new_value = self.simple_value.get()
                    if not new_value:
                        raise ValueError("请输入填充值")

                    # 执行简单填充
                    self.df[col] = new_value
                    desc = f"{col}: 填充空白值为 '{new_value}'"

                else:  # 关联填充
                    link_field = self.link_field_var.get()
                    if not link_field:
                        raise ValueError("请选择关联字段")

                    # 收集填充规则
                    rules = {}
                    for child in self.link_rules_tree.get_children():
                        link_value = self.link_rules_tree.item(child)['values'][0]
                        fill_value = self.link_rules_tree.item(child)['values'][1]
                        rules[link_value] = fill_value

                    if not rules:
                        raise ValueError("请至少设置一条填充规则")

                    # 执行关联填充
                    self.df[col] = self.df.apply(
                        lambda row: rules.get(str(row[link_field]), np.nan) if pd.isna(row[col]) else row[col],
                        axis=1
                    )

                    desc = f"{col}: 根据字段 '{link_field}' 的值填充 {len(rules)} 条规则"

                # 记录操作历史
                self.record_operation('fill', {
                    'column': col,
                    'original': original_data,
                    'new': self.df[col].copy()
                })

                # 记录操作
                self.record_operation('填充', desc)

                messagebox.showinfo("完成", f"已填充字段 '{col}'")
                fill_win.destroy()
                self.create_column_ui()

            except Exception as e:
                messagebox.showerror("错误", str(e))

        ttk.Button(btn_frame, text="确认填充", command=execute_fill).pack(side='right')

    def add_link_rule(self, target_col):
        """添加新的关联填充规则"""
        link_field = self.link_field_var.get()
        if not link_field:
            messagebox.showwarning("警告", "请先选择关联字段")
            return

        # 获取关联字段的所有唯一值
        unique_values = self.df[link_field].unique()

        rule_win = tk.Toplevel(self.root)
        rule_win.title("添加填充规则")

        ttk.Label(rule_win, text=f"当 '{link_field}' 等于:").grid(row=0, column=0, padx=5, pady=5)

        link_value_var = tk.StringVar()
        link_combobox = ttk.Combobox(rule_win, textvariable=link_value_var,
                                     values=unique_values, state='normal')
        link_combobox.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(rule_win, text=f"则 '{target_col}' 填充为:").grid(row=1, column=0, padx=5, pady=5)

        fill_value_var = tk.StringVar()
        ttk.Entry(rule_win, textvariable=fill_value_var).grid(row=1, column=1, padx=5, pady=5)

        def add_rule():
            link_value = link_value_var.get()
            fill_value = fill_value_var.get()

            if not link_value or not fill_value:
                messagebox.showwarning("警告", "请填写完整规则")
                return

            # 添加到规则树
            self.link_rules_tree.insert('', 'end', values=(link_value, fill_value))
            rule_win.destroy()

        ttk.Button(rule_win, text="添加", command=add_rule).grid(row=2, columnspan=2, pady=10)

    def remove_link_rule(self):
        """删除选中的关联填充规则"""
        selected = self.link_rules_tree.selection()
        if not selected:
            messagebox.showwarning("警告", "请先选择要删除的规则")
            return

        for item in selected:
            self.link_rules_tree.delete(item)

    def update_link_rules(self):
        """更新关联填充规则列表"""
        link_field = self.link_field_var.get()
        if not link_field:
            return

        # 清空现有规则
        for item in self.link_rules_tree.get_children():
            self.link_rules_tree.delete(item)

        # 添加关联字段的示例值
        sample_values = self.df[link_field].dropna().unique()[:5]  # 只显示前5个示例值
        for value in sample_values:
            self.link_rules_tree.insert('', 'end', values=(value, "在此输入填充值"))

    def record_operation(self, op_type, description):
        """修复后的操作记录方法"""
        # 限制历史记录条数（防止内存溢出）
        MAX_HISTORY = 50
        if len(self.operation_history) >= MAX_HISTORY:
            self.operation_history.pop(0)
        self.operation_history.append({
            'type': op_type,
            'description': description,
            'timestamp': datetime.now()  # 使用正确的datetime引用
        })
        self.update_history_display()

    def open_replace_window(self, col):
        replace_win = tk.Toplevel(self.root)
        replace_win.title(f"替换操作 - {col}")
        replace_win.geometry("800x600")

        # 记录替换前的数据状态
        original_data = self.df[col].copy()

        notebook = ttk.Notebook(replace_win)
        notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # 标签页1: 简单替换
        simple_frame = ttk.Frame(notebook)
        notebook.add(simple_frame, text="简单替换")

        # 使用Treeview显示值分布
        tree_frame = ttk.Frame(simple_frame)
        tree_frame.pack(fill='both', expand=True, padx=10, pady=5)

        tree = ttk.Treeview(tree_frame, columns=('count',), selectmode='extended')
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)

        tree.heading('#0', text='当前值')
        tree.heading('count', text='出现次数')
        tree.column('count', width=100, anchor='center')

        # 统计值分布
        value_counts = self.df[col].value_counts(dropna=False).reset_index()
        value_counts.columns = ['value', 'count']

        for _, row in value_counts.iterrows():
            tree.insert('', 'end', text=row['value'], values=(row['count'],))

        tree.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')

        # 替换参数输入
        input_frame = ttk.Frame(simple_frame)
        input_frame.pack(fill='x', padx=10, pady=5)

        ttk.Label(input_frame, text="替换为:").pack(side='left')
        new_value_entry = ttk.Entry(input_frame)
        new_value_entry.pack(side='left', expand=True, fill='x', padx=5)

        # 标签页2: 关联替换
        link_frame = ttk.Frame(notebook)
        notebook.add(link_frame, text="关联替换")

        # 关联字段选择
        link_field_frame = ttk.LabelFrame(link_frame, text="选择关联字段")
        link_field_frame.pack(fill='x', padx=5, pady=5)

        self.link_field_var = tk.StringVar()
        link_combobox = ttk.Combobox(link_field_frame, textvariable=self.link_field_var,
                                     state='readonly')
        link_combobox.pack(fill='x', padx=5, pady=5)

        # 替换规则设置
        rule_frame = ttk.LabelFrame(link_frame, text="设置替换规则")
        rule_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # 使用Treeview显示和编辑替换规则
        columns = ('original_value', 'link_field', 'link_value', 'new_value')
        self.replace_rules_tree = ttk.Treeview(rule_frame, columns=columns, show='headings', height=5)

        self.replace_rules_tree.heading('original_value', text='原值')
        self.replace_rules_tree.heading('link_field', text='关联字段')
        self.replace_rules_tree.heading('link_value', text='关联值')
        self.replace_rules_tree.heading('new_value', text='新值')

        vsb = ttk.Scrollbar(rule_frame, orient="vertical", command=self.replace_rules_tree.yview)
        hsb = ttk.Scrollbar(rule_frame, orient="horizontal", command=self.replace_rules_tree.xview)
        self.replace_rules_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.replace_rules_tree.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')
        hsb.pack(side='bottom', fill='x')

        # 添加/删除规则按钮
        rule_btn_frame = ttk.Frame(rule_frame)
        rule_btn_frame.pack(fill='x', pady=5)

        ttk.Button(rule_btn_frame, text="添加规则",
                   command=lambda: self.add_replace_rule(col)).pack(side='left', padx=2)
        ttk.Button(rule_btn_frame, text="删除规则",
                   command=self.remove_replace_rule).pack(side='left', padx=2)

        # 动态更新关联字段选项
        def update_link_fields():
            available_cols = [c for c in self.df.columns if c != col]
            link_combobox['values'] = available_cols
            if available_cols:
                self.link_field_var.set(available_cols[0])

        update_link_fields()

        # 确认按钮
        btn_frame = ttk.Frame(replace_win)
        btn_frame.pack(fill='x', padx=10, pady=10)

        def execute_replace():
            try:
                # 获取当前选择的标签页
                current_tab = notebook.index(notebook.select())

                if current_tab == 0:  # 简单替换
                    selected = [tree.item(i, 'text') for i in tree.selection()]
                    new_value = new_value_entry.get()

                    if not selected or not new_value:
                        messagebox.showwarning("警告", "请先选择要替换的值并输入新值")
                        return

                    # 执行简单替换
                    self.df[col] = self.df[col].replace(selected, new_value)
                    desc = f"{col}: 替换 {len(selected)}个值为 '{new_value}'"

                else:  # 关联替换
                    # 收集替换规则
                    rules = []
                    for child in self.replace_rules_tree.get_children():
                        values = self.replace_rules_tree.item(child)['values']
                        rules.append({
                            'original': values[0],
                            'link_field': values[1],
                            'link_value': values[2],
                            'new': values[3]
                        })

                    if not rules:
                        raise ValueError("请至少设置一条替换规则")

                    # 执行关联替换
                    for rule in rules:
                        mask = (self.df[col] == rule['original']) & (self.df[rule['link_field']] == rule['link_value'])
                        self.df.loc[mask, col] = rule['new']

                    desc = f"{col}: 根据关联字段执行了 {len(rules)} 条替换规则"

                # 记录操作历史
                self.record_operation('replace', {
                    'column': col,
                    'original': original_data,
                    'new': self.df[col].copy()
                })

                # 记录操作
                self.record_operation('替换', desc)

                messagebox.showinfo("完成", "替换操作已完成")
                replace_win.destroy()
                self.create_column_ui()

            except Exception as e:
                messagebox.showerror("错误", str(e))

        ttk.Button(btn_frame, text="执行替换", command=execute_replace).pack(side='right')

    def add_replace_rule(self, target_col):
        """添加新的关联替换规则"""
        link_field = self.link_field_var.get()
        if not link_field:
            messagebox.showwarning("警告", "请先选择关联字段")
            return

        rule_win = tk.Toplevel(self.root)
        rule_win.title("添加替换规则")
        rule_win.geometry("500x400")

        # 原值选择
        ttk.Label(rule_win, text="要替换的原值:").grid(row=0, column=0, padx=5, pady=5, sticky='w')
        original_var = tk.StringVar()
        original_combobox = ttk.Combobox(rule_win, textvariable=original_var)
        original_combobox.grid(row=0, column=1, padx=5, pady=5, sticky='ew')

        # 修改这里：使用 dropna=False 保留空值，并将 NaN 转换为字符串 "NaN" 或 "空值"
        unique_values = self.df[target_col].unique()
        # 将 NaN 转换为字符串表示
        unique_values = [str(x) if pd.notna(x) else "空值" for x in unique_values]
        original_combobox['values'] = unique_values

        # 关联字段值
        ttk.Label(rule_win, text=f"当 '{link_field}' 等于:").grid(row=1, column=0, padx=5, pady=5, sticky='w')
        link_value_var = tk.StringVar()
        link_value_combobox = ttk.Combobox(rule_win, textvariable=link_value_var)
        link_value_combobox.grid(row=1, column=1, padx=5, pady=5, sticky='ew')

        # 同样处理关联字段的空值
        link_unique_values = self.df[link_field].unique()
        link_unique_values = [str(x) if pd.notna(x) else "空值" for x in link_unique_values]
        link_value_combobox['values'] = link_unique_values

        # 新值
        ttk.Label(rule_win, text="替换为:").grid(row=2, column=0, padx=5, pady=5, sticky='w')
        new_value_var = tk.StringVar()
        ttk.Entry(rule_win, textvariable=new_value_var).grid(row=2, column=1, padx=5, pady=5, sticky='ew')

        def add_rule():
            original_value = original_var.get()
            link_value = link_value_var.get()
            new_value = new_value_var.get()

            if not all([original_value, link_value, new_value]):
                messagebox.showwarning("警告", "请填写完整规则")
                return

            # 添加到规则树
            self.replace_rules_tree.insert('', 'end',
                                           values=(original_value, link_field, link_value, new_value))
            rule_win.destroy()

        ttk.Button(rule_win, text="添加", command=add_rule).grid(row=3, columnspan=2, pady=10)

    def remove_replace_rule(self):
        """删除选中的替换规则"""
        selected = self.replace_rules_tree.selection()
        if not selected:
            messagebox.showwarning("警告", "请先选择要删除的规则")
            return

        for item in selected:
            self.replace_rules_tree.delete(item)
    def open_filter_window(self, col):
        filter_win = tk.Toplevel(self.root)
        filter_win.title(f"文本筛选 - {col}")

        # 记录筛选前的掩码状态
        original_mask = self.current_mask.copy()

        # 条件类型选择
        cond_frame = ttk.Frame(filter_win)
        cond_frame.pack(fill='x', padx=10, pady=5)

        ttk.Label(cond_frame, text="条件类型:").pack(side='left')
        condition_var = tk.StringVar(value="等于")
        conditions = ttk.Combobox(cond_frame, textvariable=condition_var,
                                  values=["等于", "不等于", "包含", "不包含"], state="readonly")
        conditions.pack(side='left', padx=5)

        # 值选择/输入区域
        input_frame = ttk.Frame(filter_win)
        input_frame.pack(fill='both', expand=True, padx=10, pady=5)

        self.filter_input_widget = None
        self.current_col = col

        def update_input_widget():
            for widget in input_frame.winfo_children():
                widget.destroy()

            condition = condition_var.get()

            if condition in ["等于", "不等于"]:
                # 多选列表框
                listbox = tk.Listbox(input_frame, selectmode=tk.MULTIPLE, height=8)
                vsb = ttk.Scrollbar(input_frame, orient="vertical", command=listbox.yview)
                listbox.configure(yscrollcommand=vsb.set)

                for value in self.df[col].unique():
                    listbox.insert('end', value)

                listbox.pack(side='left', fill='both', expand=True)
                vsb.pack(side='right', fill='y')
                self.filter_input_widget = listbox
            else:
                # 文本输入框
                entry = ttk.Entry(input_frame)
                entry.pack(fill='x', pady=2)
                ttk.Label(input_frame, text="多个值用逗号分隔").pack()
                self.filter_input_widget = entry

        condition_var.trace_add('write', lambda *_: update_input_widget())
        update_input_widget()

        def apply_filter():
            try:
                condition = condition_var.get()
                mask = pd.Series([True] * len(self.df))

                if condition in ["等于", "不等于"]:
                    selected = [self.filter_input_widget.get(i)
                                for i in self.filter_input_widget.curselection()]
                    if not selected:
                        raise ValueError("请至少选择一个筛选值")

                    mask = self.df[col].isin(selected)
                    if condition == "不等于":
                        mask = ~mask

                    # 修正描述逻辑：不涉及keywords
                    cond_desc = f"{condition} {len(selected)}个值"

                else:
                    keywords = [k.strip() for k in self.filter_input_widget.get().split(',') if k.strip()]
                    if not keywords:
                        raise ValueError("请输入至少一个关键词")

                    pattern = '|'.join(map(re.escape, keywords))
                    contains = self.df[col].str.contains(pattern, na=False, regex=True)
                    mask = contains if condition == "包含" else ~contains

                    # 修正描述逻辑：仅在此分支使用keywords
                    cond_desc = f"{condition}关键词: {', '.join(keywords)}"

                # 记录操作历史（使用修正后的cond_desc）
                self.record_operation('筛选', f"{col}: {cond_desc}")

                self.current_mask = self.current_mask & mask
                messagebox.showinfo("成功", "筛选条件已应用")
                filter_win.destroy()
            except Exception as e:
                messagebox.showerror("错误", str(e))

        ttk.Button(filter_win, text="应用筛选", command=apply_filter).pack(pady=5)

    def undo_operation(self):
        """撤销最近一次操作"""
        if not self.operation_history:
            messagebox.showinfo("提示", "没有可撤销的操作历史")
            return

        last_op = self.operation_history.pop()
        if not self.operation_history:
            messagebox.showinfo("提示", "没有可撤销的操作历史")
            return

        last_op = self.operation_history.pop()

        if last_op['type'] == 'filter':
            # 恢复筛选前的掩码状态
            self.current_mask = last_op['data']['original_mask']
            messagebox.showinfo("撤销成功", "已撤销最后一次筛选操作")
        elif last_op['type'] == 'replace':
            # 恢复替换前的列数据
            col = last_op['data']['column']
            self.df[col] = last_op['data']['original']
            messagebox.showinfo("撤销成功", f"已撤销对字段 {col} 的替换操作")

        self.create_column_ui()
        self.update_history_display()

    def reset_all(self):
        """重置所有条件和数据"""
        self.df = self.original_df.copy()
        self.current_mask = pd.Series([True] * len(self.df))
        self.operation_history = []
        self.create_column_ui()
        messagebox.showinfo("重置成功", "已重置所有数据和筛选条件")
        self.record_operation('系统', "重置所有操作")

    def export_data(self):
        export_win = tk.Toplevel(self.root)  # 必须首先创建窗口对象
        export_win.title("导出设置")

        # 数据量选择框架
        count_frame = ttk.LabelFrame(export_win, text="选择导出数据量")
        count_frame.pack(padx=10, pady=5, fill='x')

        # 导出选项变量
        self.export_option = tk.StringVar(value="all")
        self.custom_count = tk.IntVar(value=1000)

        # 选项按钮
        ttk.Radiobutton(count_frame, text="导出全部数据",
                        variable=self.export_option, value="all").pack(anchor='w')
        ttk.Radiobutton(count_frame, text="自定义数量",
                        variable=self.export_option, value="custom").pack(anchor='w')

        # 自定义数量输入
        count_entry = ttk.Entry(count_frame, textvariable=self.custom_count, width=10)
        count_entry.pack(side='left', padx=5)
        ttk.Label(count_frame, text="条记录").pack(side='left')

        # 使用nonlocal声明确保闭包访问
        def confirm_export():
            nonlocal export_win  # 关键修复：声明使用外部变量
            try:
                filtered_df = self.df[self.current_mask]

                if self.export_option.get() == "custom":
                    max_count = len(filtered_df)
                    want_count = self.custom_count.get()

                    if want_count <= 0:
                        raise ValueError("数量必须大于0")
                    if want_count > max_count:
                        raise ValueError(f"最大可导出数量：{max_count}")

                    filtered_df = filtered_df.head(want_count)

                save_path = filedialog.asksaveasfilename(
                    defaultextension=".csv",
                    filetypes=[("CSV Files", "*.csv")]
                )
                if save_path:
                    filtered_df.to_csv(save_path, index=False)
                    messagebox.showinfo("导出成功", f"已成功导出{len(filtered_df)}条记录")
                    export_win.destroy()  # 确保正确引用窗口对象

            except Exception as e:
                messagebox.showerror("导出错误", str(e))

        # 按钮必须在窗口对象创建后定义
        ttk.Button(export_win, text="确认导出", command=confirm_export).pack(pady=10)


if __name__ == "__main__":
    app = FinalCSVProcessor()
    app.root.mainloop()