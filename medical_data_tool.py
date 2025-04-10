# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import glob
import re
import numpy as np
import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
                             QPushButton, QTextEdit, QLabel, QFileDialog, QWidget,
                             QLineEdit, QProgressBar, QMessageBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QUrl
from PyQt5.QtGui import QDesktopServices


class ExcelProcessor(QThread):
    """数据处理线程类，负责在后台执行数据提取和处理任务"""
    update_signal = pyqtSignal(str)  # 日志更新信号
    finished_signal = pyqtSignal(str, list)  # 任务完成信号

    def __init__(self, input_folder, output_path):
        super().__init__()
        self.input_folder = input_folder
        self.output_path = output_path
        self.current_time = datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
        self._is_running = True  # 线程运行状态标志

    def stop(self):
        """安全停止线程的方法"""
        self._is_running = False
        self.terminate()

    def log(self, message):
        """线程安全的日志记录方法"""
        if self._is_running:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            self.update_signal.emit(f"[{timestamp}]  {message}")

    def run(self):
        """主处理逻辑"""
        try:
            # 关键修复：每次运行前显式切换工作目录
            os.chdir(self.input_folder)
            self.log(
                f"\n{'=' * 60}\n 医疗数据提取工具运行日志\n开始时间: {self.current_time}\n 输入目录: {os.getcwd()}\n 输出目录: {self.output_path}\n{'=' * 60}")

            # 配置文件匹配模式
            FILE_PATTERNS = {
                '医院感染汇总表': r'医院感染汇总表[^\.]*\.(xlsx|xls)$',
                '医院感染现患率': r'医院感染现患率[^\.]*\.(xlsx|xls)$',
                '手卫生依从正确率': r'手卫生依从正确率[^\.]*\.(xlsx|xls)$',
                'I类切口手术部位感染率': r'I类切口手术部位感染率[^\.]*\.(xlsx|xls)$',
                '血管导管相关血流感染发病率': r'血管导管相关血流感染发病率[^\.]*\.(xlsx|xls)$',
                '呼吸机相关肺炎发病率': r'呼吸机相关肺炎发病率[^\.]*\.(xlsx|xls)$',
                '导尿管相关泌尿道感染发病率': r'导尿管相关泌尿道感染发病率[^\.]*\.(xlsx|xls)$',
            }

            # 指标映射配置
            INDICATOR_MAP = {
                '医院感染汇总表': ['新发感染人数', '新发感染例次数', '同期住院患者人数', '漏报病例数'],
                '医院感染现患率': ['感染人数', '感染例次数', '现患率-同期住院患者人数'],
                '手卫生依从正确率': ['实际实施手卫生次数', '应实施手卫生次数'],
                'I类切口手术部位感染率': ['Ⅰ类手术部位感染例次数', 'Ⅰ类手术例数'],
                '血管导管相关血流感染发病率': ['血管导管相关血流感染例次数', '中心静脉插管使用天数'],
                '呼吸机相关肺炎发病率': ['呼吸机相关肺炎感染例次数', '呼吸机使用天数'],
                '导尿管相关泌尿道感染发病率': ['导尿管相关泌尿道感染例次数', '导尿管使用天数']
            }

            # 指标名称标准化映射
            INDICATOR_NAME_MAPPING = {
                "新发感染人数": ["医院感染新发病例数", "同期应报告医院感染病例总数"],
                "新发感染例次数": ["医院感染新发例次数"],
                "同期住院患者人数": ["同期住院患者人数"],
                "现患率-同期住院患者人数": ["现患率-同期住院患者总数"],
                "感染人数": ["确定时段或时点医院感染患者数"],
                "感染例次数": ["确定时段或时点医院感染例次数"],
                "漏报病例数": ["应当报告而未报告的医院感染病例数"],
                "实际实施手卫生次数": ["受调查的医务人员实际实施手卫生次数"],
                "应实施手卫生次数": ["同期调查中应实施手卫生次数"],
                "Ⅰ类手术部位感染例次数": ["发生I类切口手术部位感染病例数"],
                "Ⅰ类手术例数": ["同期接受I类切口手术患者总数"],
                "血管导管相关血流感染例次数": ["血管内导管相关血流感染例次数"],
                "中心静脉插管使用天数": ["同期患者使用血管内导管留置总天数"],
                "呼吸机相关肺炎感染例次数": ["呼吸机相关肺炎例次数"],
                "呼吸机使用天数": ["同期患者使用呼吸机总天数"],
                "导尿管相关泌尿道感染例次数": ["导尿管相关泌尿系感染例次数"],
                "导尿管使用天数": ["同期患者使用导尿管总天数"]
            }

            # 结果输出顺序定义
            RESULT_ROW_ORDER = [
                "医院感染新发病例数", "医院感染新发例次数", "同期住院患者人数",
                "确定时段或时点医院感染患者数", "确定时段或时点医院感染例次数",
                "现患率-同期住院患者总数", "应当报告而未报告的医院感染病例数",
                "同期应报告医院感染病例总数", "受调查的医务人员实际实施手卫生次数",
                "同期调查中应实施手卫生次数", "发生I类切口手术部位感染病例数",
                "同期接受I类切口手术患者总数", "血管内导管相关血流感染例次数",
                "同期患者使用血管内导管留置总天数", "呼吸机相关肺炎例次数",
                "同期患者使用呼吸机总天数", "导尿管相关泌尿系感染例次数",
                "同期患者使用导尿管总天数"
            ]

            def find_header_row(df, file_type):
                """定位数据表的表头行"""
                target_indicators = INDICATOR_MAP.get(file_type, [])
                for i, row in df.iterrows():
                    row_str = '|'.join([str(x) for x in row if pd.notna(x)])
                    if any(indicator in row_str for indicator in target_indicators):
                        self.log(
                            f" 在文件类型[{file_type}]中发现表头行: 第{i + 1}行 (包含指标: {[ind for ind in target_indicators if ind in row_str]})")
                        return i
                self.log(f" 警告: 未能在[{file_type}]中找到明确表头行，默认使用第一行")
                return 0

            def clean_dataframe(df, file_type):
                """数据清洗和预处理"""
                header_row = find_header_row(df, file_type)
                original_columns = [str(col) for col in df.columns]
                df.columns = df.iloc[header_row]
                df = df[header_row + 1:]
                self.log(
                    f" 数据清洗: 原始列名[{original_columns[:3]}...] → 新列名[{df.columns[:3]}...]  (共{len(df.columns)} 列)")

                # 特殊处理现患率表的列名
                if file_type == '医院感染现患率':
                    df.columns = [
                        col.replace('同期住院患者人数', '现患率-同期住院患者人数') if '同期住院患者人数' in str(
                            col) else col for col in df.columns]
                    self.log(" 特殊处理: 已将'同期住院患者人数'重命名为'现患率-同期住院患者人数'")

                # 清理空行空列
                df = df.dropna(how='all').dropna(how='all', axis=1)
                df = df.reset_index(drop=True)

                # 确保第一列是科室/全院列
                first_col = df.columns[0]
                if not any(x in str(first_col) for x in ['科室', '全院', '合计']):
                    for col in df.columns:
                        if any(x in str(col) for x in ['科室', '全院', '合计']):
                            cols = df.columns.tolist()
                            cols.insert(0, cols.pop(cols.index(col)))
                            df = df[cols]
                            self.log(f" 列顺序调整: 将'{col}'移动到第一列位置")
                            break
                return df

            def find_quanyuan_row(df):
                """定位包含全院数据的行"""
                first_col = df.columns[0]
                patterns = ['全院', '合计', '总计', '汇总', '全院合计']
                for pattern in patterns:
                    matches = df[df[first_col].astype(str).str.contains(pattern, na=False)]
                    if not matches.empty:
                        self.log(f" 成功定位全院数据行: 匹配模式'{pattern}' (值: {matches.iloc[0][first_col]})")
                        return matches.iloc[0]
                if df.shape[0] == 1:
                    self.log(" 数据只有一行，自动作为全院数据")
                    return df.iloc[0]
                self.log(" 警告: 未找到明确的全院数据行，请检查数据")
                return None

                # === 文件扫描阶段 ===

            self.log("\n===  文件扫描阶段 ===")
            excel_files = {}
            for file_type, pattern in FILE_PATTERNS.items():
                # 使用绝对路径匹配文件
                matched_files = [f for f in glob.glob(os.path.join(self.input_folder, '*.xls*'))
                                 if re.search(pattern, os.path.basename(f), re.IGNORECASE)
                                 and not os.path.basename(f).startswith('  汇总')]
                if matched_files:
                    excel_files[file_type] = matched_files
                    self.log(f" 成功匹配[{file_type}]文件: {', '.join([os.path.basename(f) for f in matched_files])}")
                else:
                    self.log(f" 未找到匹配[{file_type}]类型的文件")

            if not excel_files:
                self.log(" 错误: 未找到任何符合要求的Excel文件，请检查输入目录")
                return

                # === 数据提取阶段 ===
            self.log("\n===  数据提取阶段 ===")
            combined_data = {}
            for file_type, files in excel_files.items():
                for file in files:
                    try:
                        self.log(f"\n>>  开始处理: {os.path.basename(file)}  [类型: {file_type}]")

                        # 尝试读取Excel文件
                        try:
                            df = pd.read_excel(file, header=None)
                            self.log(f" 文件读取成功 (尺寸: {df.shape[0]} 行×{df.shape[1]} 列)")
                        except Exception as e:
                            self.log(f" 标准读取失败: {str(e)}，尝试使用openpyxl引擎...")
                            df = pd.read_excel(file, header=None, engine='openpyxl')
                            self.log(f"openpyxl 引擎读取成功 (尺寸: {df.shape[0]} 行×{df.shape[1]} 列)")

                        # 数据清洗
                        df = clean_dataframe(df, file_type)
                        if df.empty:
                            self.log(" 警告: 清理后数据为空，跳过此文件")
                            continue

                            # 定位全院数据行
                        row = find_quanyuan_row(df)
                        if row is None:
                            self.log(f" 错误: 无法定位全院数据行，首列值: {df[df.columns[0]].unique()}")
                            continue

                            # 提取指标数据
                        indicators = INDICATOR_MAP.get(file_type, [])
                        extracted_count = 0
                        for col in df.columns:
                            col_str = str(col).strip()
                            for indicator in indicators:
                                if indicator in col_str:
                                    try:
                                        value = row[col]
                                        if pd.isna(value):
                                            self.log(f" 跳过空值: 指标[{indicator}]在列[{col_str}]")
                                            continue

                                            # 数值类型转换
                                        if isinstance(value, (np.int64, np.int32, np.float64)):
                                            value = float(value)
                                            self.log(f" 数值转换: {indicator} = {value}")

                                        # 指标名称标准化
                                        if indicator in INDICATOR_NAME_MAPPING:
                                            for new_name in INDICATOR_NAME_MAPPING[indicator]:
                                                combined_data[new_name] = value
                                                extracted_count += 1
                                                self.log(f" 成功提取: {indicator} → 映射为[{new_name}] = {value}")
                                        else:
                                            combined_data[indicator] = value
                                            extracted_count += 1
                                            self.log(f" 成功提取: {indicator} = {value}")
                                        break
                                    except Exception as e:
                                        self.log(f" 提取失败 [{indicator}]: {str(e)}")
                        self.log(f"<<  处理完成: 共提取{extracted_count}个指标")

                    except Exception as e:
                        self.log(f" 处理文件异常: {str(e)}")
                        continue

            if not combined_data:
                self.log("\n 错误: 未提取到任何有效数据，请检查文件格式")
                return

                # === 结果整理阶段 ===
            self.log("\n===  结果整理阶段 ===")
            result = pd.DataFrame(combined_data, index=["全院"]).T
            result.columns = ["全院"]

            final_result = pd.DataFrame()
            ordered_results = []
            for indicator in RESULT_ROW_ORDER:
                if indicator in combined_data:
                    final_result = pd.concat([final_result, result.loc[[indicator]]])
                    ordered_results.append((indicator, combined_data[indicator]))
                    self.log(f" 按顺序添加指标: {indicator} = {combined_data[indicator]}")

            # 生成带时间戳的输出文件名
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_path, f"医疗数据汇总结果_{timestamp}.xlsx")
            final_result.to_excel(output_file, index=True)

            # === 输出结果 ===
            self.log(f"\n===  输出结果 ===")
            self.log(f" 生成汇总文件: {output_file}")
            self.log(f" 包含指标数量: {len(final_result)}个")
            self.log(f" 文件尺寸: {os.path.getsize(output_file) / 1024:.2f}  KB")
            self.log("\n 处理流程完成!")

            if self._is_running:
                self.finished_signal.emit(output_file, ordered_results)

        except Exception as e:
            self.log(f"\n 严重错误: {str(e)}")
            if self._is_running:
                self.finished_signal.emit("", [])


class MainWindow(QMainWindow):
    """主界面类"""

    def __init__(self):
        super().__init__()
        self.initUI()
        self.processor = None
        self.output_file = ""

    def initUI(self):
        """初始化用户界面"""
        self.setWindowTitle(' 医疗数据提取工具-欧民鑫制作')
        self.setGeometry(300, 300, 900, 700)

        main_widget = QWidget()
        layout = QVBoxLayout()

        # === 输入区域 ===
        input_layout = QHBoxLayout()
        self.input_label = QLabel("输入文件夹:")
        self.input_line = QLineEdit()
        self.input_line.setPlaceholderText(" 请选择包含医疗Excel文件的文件夹")
        self.input_btn = QPushButton("浏览...")
        self.input_btn.clicked.connect(self.select_input_folder)
        input_layout.addWidget(self.input_label)
        input_layout.addWidget(self.input_line)
        input_layout.addWidget(self.input_btn)

        # === 输出区域 ===
        output_layout = QHBoxLayout()
        self.output_label = QLabel("输出路径:")
        self.output_line = QLineEdit()
        self.output_line.setPlaceholderText(" 请选择结果保存位置")
        self.output_btn = QPushButton("浏览...")
        self.output_btn.clicked.connect(self.select_output_folder)
        output_layout.addWidget(self.output_label)
        output_layout.addWidget(self.output_line)
        output_layout.addWidget(self.output_btn)

        # === 操作按钮区域 ===
        btn_layout = QHBoxLayout()
        self.run_btn = QPushButton("开始提取数据")
        self.run_btn.setStyleSheet("background-color:  #4CAF50; color: white; font-weight: bold;")
        self.run_btn.clicked.connect(self.run_extraction)

        self.reset_btn = QPushButton("重置运行")
        self.reset_btn.setStyleSheet("background-color:  #f44336; color: white;")
        self.reset_btn.clicked.connect(self.reset_operation)

        self.open_btn = QPushButton("打开结果位置")
        self.open_btn.setStyleSheet("background-color:  #2196F3; color: white;")
        self.open_btn.clicked.connect(self.open_output_folder)
        self.open_btn.setEnabled(False)

        btn_layout.addWidget(self.run_btn)
        btn_layout.addWidget(self.reset_btn)
        btn_layout.addWidget(self.open_btn)

        # === 进度条 ===
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setStyleSheet("QProgressBar  {text-align: center;}")

        # === 日志输出 ===
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("font-family:  'Courier New'; font-size: 11px;")
        self.log_output.setLineWrapMode(QTextEdit.NoWrap)

        # 添加到主布局
        layout.addLayout(input_layout)
        layout.addLayout(output_layout)
        layout.addLayout(btn_layout)
        layout.addWidget(self.progress)
        layout.addWidget(QLabel(" 运行日志:"))
        layout.addWidget(self.log_output)

        main_widget.setLayout(layout)
        self.setCentralWidget(main_widget)

        # 初始日志
        self.log_output.append(f"[ 系统] 应用程序启动 - {datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
        self.log_output.append("[ 提示] 请先选择输入文件夹和输出路径")

    def log(self, message):
        """记录日志到界面"""
        self.log_output.append(message)

    def select_input_folder(self):
        """选择输入文件夹"""
        folder = QFileDialog.getExistingDirectory(self, "选择输入文件夹")
        if folder:
            self.input_line.setText(folder)
            self.log(f"[ 操作] 已选择输入文件夹: {folder}")
            if not self.output_line.text():
                default_output = os.path.join(folder, "输出结果")
                self.output_line.setText(default_output)
                self.log(f"[ 系统] 自动设置输出路径: {default_output}")

    def select_output_folder(self):
        """选择输出文件夹"""
        folder = QFileDialog.getExistingDirectory(self, "选择输出文件夹")
        if folder:
            self.output_line.setText(folder)
            self.log(f"[ 操作] 已选择输出文件夹: {folder}")

    def open_output_folder(self):
        """打开结果所在文件夹"""
        if self.output_file and os.path.exists(self.output_file):
            folder = os.path.dirname(self.output_file)
            QDesktopServices.openUrl(QUrl.fromLocalFile(folder))
            self.log(f"[ 操作] 已打开结果文件夹: {folder}")
        else:
            QMessageBox.warning(self, "警告", "结果文件不存在或尚未生成")

    def reset_operation(self):
        """完全重置运行状态"""
        # 终止正在运行的任务
        if self.processor and self.processor.isRunning():
            self.processor.stop()
            self.processor.wait()

            # 重置所有界面状态
        self.input_line.clear()
        self.output_line.clear()
        self.progress.setValue(0)
        self.log_output.clear()
        self.output_file = ""

        # 重置按钮状态
        self.run_btn.setEnabled(True)
        self.open_btn.setEnabled(False)

        # 重新初始化日志
        current_time = datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
        self.log_output.append(f"[ 系统] 系统已完全重置 - {current_time}")
        self.log_output.append("[ 提示] 所有选择和日志已被清空")
        self.log_output.append("[ 提示] 可以开始新的数据处理任务")

    def run_extraction(self):
        """启动数据处理任务"""
        input_folder = self.input_line.text()
        output_folder = self.output_line.text()

        # 输入验证
        if not input_folder or not output_folder:
            QMessageBox.critical(self, "错误", "请先选择输入文件夹和输出路径")
            return

        if not os.path.exists(input_folder):
            QMessageBox.critical(self, "错误", f"输入文件夹不存在: {input_folder}")
            return

            # 创建输出目录（如果不存在）
        if not os.path.exists(output_folder):
            try:
                os.makedirs(output_folder)
                self.log(f"[ 系统] 已创建输出文件夹: {output_folder}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"无法创建输出文件夹: {str(e)}")
                return

                # 准备运行任务
        self.log("\n[ 系统] 开始数据处理...")
        self.run_btn.setEnabled(False)
        self.open_btn.setEnabled(False)
        self.progress.setValue(10)

        # 创建并启动处理线程
        self.processor = ExcelProcessor(input_folder, output_folder)
        self.processor.update_signal.connect(self.log)
        self.processor.finished_signal.connect(self.extraction_finished)
        self.processor.start()

    def extraction_finished(self, output_file, ordered_results):
        """处理完成后的回调"""
        self.progress.setValue(100)
        self.run_btn.setEnabled(True)

        if output_file:
            self.output_file = output_file
            self.open_btn.setEnabled(True)
            self.log("\n[ 系统] 数据处理完成!")
            self.log(f"[ 结果] 已保存到: {output_file}")

            # 按结果顺序显示摘要
            self.log("\n[ 结果摘要 - 按输出顺序]")
            for indicator, value in ordered_results:
                self.log(f"{indicator}:  {value}")

            QMessageBox.information(self, "完成", f"结果已保存到:\n{output_file}")
        else:
            self.log("\n[ 错误] 数据处理失败，请检查日志")
            QMessageBox.critical(self, "错误", "数据处理失败，请检查日志")


def main():
    """应用程序入口"""
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()