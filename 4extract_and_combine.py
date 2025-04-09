# -*- coding: utf-8 -*-
"""
Excel数据提取脚本
功能：从多个Excel文件中提取指定指标数据，并汇总到一个文件中
"""

# 导入常用库
import pandas as pd  # 用于数据处理
import os  # 用于处理文件路径
import glob  # 用于查找匹配的文件
import re  # 正则表达式库，用于匹配文件名
import numpy as np  # 处理数值型数据

# ================= 配置区域 =================

# 文件名与正则匹配模式字典：匹配不同类型的Excel报表文件
FILE_PATTERNS = {
    '医院感染汇总表': r'医院感染汇总表[^\.]*\.(xlsx|xls)$',
    '医院感染现患率': r'医院感染现患率[^\.]*\.(xlsx|xls)$',
    '手卫生依从正确率': r'手卫生依从正确率[^\.]*\.(xlsx|xls)$',
    'I类切口手术部位感染率': r'I类切口手术部位感染率[^\.]*\.(xlsx|xls)$',
    '血管导管相关血流感染发病率': r'血管导管相关血流感染发病率[^\.]*\.(xlsx|xls)$',
    '呼吸机相关肺炎发病率': r'呼吸机相关肺炎发病率[^\.]*\.(xlsx|xls)$',
    '导尿管相关泌尿道感染发病率': r'导尿管相关泌尿道感染发病率[^\.]*\.(xlsx|xls)$',
}

# 每类文件中需要提取的指标字段（列标题）
INDICATOR_MAP = {
    '医院感染汇总表': ['新发感染人数', '新发感染例次数', '同期住院患者人数', '漏报病例数'],
    '医院感染现患率': ['感染人数', '感染例次数'],
    '手卫生依从正确率': ['实际实施手卫生次数', '应实施手卫生次数'],
    'I类切口手术部位感染率': ['Ⅰ类手术部位感染例次数', 'Ⅰ类手术例数'],
    '血管导管相关血流感染发病率': ['血管导管相关血流感染例次数', '中心静脉插管使用天数'],
    '呼吸机相关肺炎发病率': ['呼吸机相关肺炎感染例次数', '呼吸机使用天数'],
    '导尿管相关泌尿道感染发病率': ['导尿管相关泌尿道感染例次数', '导尿管使用天数']
}

# 汇总结果中行的显示顺序（部分指标可能重复是为了位置对齐）
RESULT_ROW_ORDER = [
    "新发感染人数",
    "新发感染例次数",
    "感染人数",
    "感染例次数",
    "漏报病例数",
    "新发感染人数",
    "实际实施手卫生次数",
    "应实施手卫生次数",
    "Ⅰ类手术部位感染例次数",
    "Ⅰ类手术例数",
    "血管导管相关血流感染例次数",
    "中心静脉插管使用天数",
    "呼吸机相关肺炎感染例次数",
    "呼吸机使用天数",
    "导尿管相关泌尿道感染例次数",
    "导尿管使用天数"
]
# ================= 配置结束 =================

def find_excel_files():
    """
    查找当前目录下所有符合预设正则规则的Excel文件。
    返回字典，键为文件类型名，值为对应的文件路径列表。
    """
    excel_files = {}
    for file_type, pattern in FILE_PATTERNS.items():
        # 查找所有Excel文件 (*.xls 或 *.xlsx)，并排除以“汇总”开头的文件
        matched_files = [f for f in glob.glob('*.xls*')
                         if re.search(pattern, f, re.IGNORECASE)
                         and not f.startswith('汇总')]
        if matched_files:
            excel_files[file_type] = matched_files
    return excel_files

def find_header_row(df, file_type):
    """
    自动查找表头所在的行号。
    遍历所有行，查找是否包含任一预设指标名。
    """
    target_indicators = INDICATOR_MAP.get(file_type, [])
    for i, row in df.iterrows():
        # 将每行合并为字符串后检查是否包含指标关键词
        row_str = '|'.join([str(x) for x in row if pd.notna(x)])
        if any(indicator in row_str for indicator in target_indicators):
            return i  # 返回该行为表头行
    return 0  # 若未找到，默认返回第一行作为表头

def clean_dataframe(df, file_type):
    """
    清洗原始DataFrame：设定表头，去除空行空列，重排列顺序。
    """
    header_row = find_header_row(df, file_type)
    df.columns = df.iloc[header_row]  # 使用表头行作为列名
    df = df[header_row + 1:]  # 去掉表头行以上的数据
    df = df.dropna(how='all').dropna(how='all', axis=1)  # 删除全空的行和列
    df = df.reset_index(drop=True)  # 重置索引

    # 保证第一列为“科室”或“全院”等字段（便于后续识别）
    if len(df.columns) > 0 and not any(x in str(df.columns[0]) for x in ['科室', '全院', '合计']):
        for col in df.columns:
            if any(x in str(col) for x in ['科室', '全院', '合计']):
                cols = df.columns.tolist()
                cols.insert(0, cols.pop(cols.index(col)))  # 将该列移至最前
                df = df[cols]
                break
    return df

def extract_data_from_file(filepath, file_type):
    """
    从单个Excel文件中提取“全院”或“合计”行的指定指标数据。
    """
    try:
        filename = os.path.basename(filepath)
        print(f"\n处理文件: {filename} (类型: {file_type})")

        # 不设表头读取，保留原始信息
        df = pd.read_excel(filepath, header=None)
        print("原始内容预览:")
        print(df.head(3))  # 打印前几行供调试查看

        df = clean_dataframe(df, file_type)
        print("清理后的内容预览:")
        print(df.head(3))

        if df.empty:
            print("警告：清理后表格为空")
            return None

        first_col = df.columns[0]  # 获取第一列的列名

        # 定位“全院”或“合计”行作为数据行
        quan_yuan_row = df[df[first_col].astype(str).str.strip().isin(["全院", "合计"])]
        if quan_yuan_row.empty:
            print(f"警告：未找到'全院'或'合计'行。首列内容:\n{df[first_col].unique()}")
            return None

        row = quan_yuan_row.iloc[0]  # 仅取第一条符合的行
        print(f"找到指标数据行: {row.values}")

        indicators = INDICATOR_MAP.get(file_type, [])
        data = {}
        for col in df.columns:
            col_str = str(col).strip()
            for indicator in indicators:
                if indicator in col_str:
                    try:
                        value = row[col]  # 提取对应单元格的值
                        if pd.isna(value):
                            continue
                        if isinstance(value, (np.int64, np.int32, np.float64)):
                            value = float(value)  # 转为浮点数
                        data[indicator] = value
                        print(f"√ 提取指标 '{indicator}': {value}")
                        break
                    except Exception as e:
                        print(f"× 提取指标 '{indicator}' 失败: {e}")
        if not data:
            print("警告：未提取任何数据。列名：", list(df.columns))
        return data
    except Exception as e:
        print(f"处理文件时出错: {e}")
        return None

def main():
    """
    主程序入口：查找文件、提取数据、汇总结果并导出为Excel。
    """
    print("开始处理Excel文件...")
    excel_files = find_excel_files()
    if not excel_files:
        print("未找到任何匹配的Excel文件")
        return

    print("\n找到的文件列表：")
    for k, v in excel_files.items():
        print(f"{k}: {v}")

    combined_data = {}  # 用于存放最终的所有提取数据
    for file_type, files in excel_files.items():
        for file in files:
            if data := extract_data_from_file(file, file_type):
                combined_data.update(data)  # 合并每个文件提取的数据

    if not combined_data:
        print("未提取到任何数据")
        return

    # 构建DataFrame，行为指标名，列为“全院”
    result = pd.DataFrame(combined_data, index=["全院"]).T
    result.columns = ["全院"]

    # 排序：按照预设顺序排列指标行
    available_rows = [r for r in RESULT_ROW_ORDER if r in combined_data]
    result = result.loc[available_rows]

    # 保存为Excel文件
    output_path = "汇总.xlsx"
    result.to_excel(output_path, index=True)
    print(f"\n成功！数据汇总已保存：{os.path.abspath(output_path)}")
    print(result)

# 判断是否作为主程序运行（而不是被导入为模块）
if __name__ == "__main__":
    main()
