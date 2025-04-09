# -*- coding: utf-8 -*-
"""
Excel数据提取脚本（完整注释版）
功能：从医疗Excel报表中提取关键指标数据，按指定顺序和名称映射输出汇总结果
"""

# 导入必要的Python库
import pandas as pd  # 数据处理和分析库
import os  # 操作系统路径处理
import glob  # 文件路径匹配
import re  # 正则表达式处理
import numpy as np  # 数值计算

# ================= 配置区域开始 =================
# 定义文件名匹配模式（正则表达式）
FILE_PATTERNS = {
    # 键：文件类型描述，值：匹配文件名的正则模式
    '医院感染汇总表': r'医院感染汇总表[^\.]*\.(xlsx|xls)$',  # 匹配"医院感染汇总表"开头的Excel文件
    '医院感染现患率': r'医院感染现患率[^\.]*\.(xlsx|xls)$',
    '手卫生依从正确率': r'手卫生依从正确率[^\.]*\.(xlsx|xls)$',
    'I类切口手术部位感染率': r'I类切口手术部位感染率[^\.]*\.(xlsx|xls)$',
    '血管导管相关血流感染发病率': r'血管导管相关血流感染发病率[^\.]*\.(xlsx|xls)$',
    '呼吸机相关肺炎发病率': r'呼吸机相关肺炎发病率[^\.]*\.(xlsx|xls)$',
    '导尿管相关泌尿道感染发病率': r'导尿管相关泌尿道感染发病率[^\.]*\.(xlsx|xls)$',
}

# 定义各文件类型需要提取的原始指标名称
INDICATOR_MAP = {
    # 键：文件类型，值：需要提取的指标列表
    '医院感染汇总表': ['新发感染人数', '新发感染例次数', '同期住院患者人数', '漏报病例数'],
    '医院感染现患率': ['感染人数', '感染例次数', '现患率-同期住院患者人数'],  # 特殊处理同名指标
    '手卫生依从正确率': ['实际实施手卫生次数', '应实施手卫生次数'],
    'I类切口手术部位感染率': ['Ⅰ类手术部位感染例次数', 'Ⅰ类手术例数'],
    '血管导管相关血流感染发病率': ['血管导管相关血流感染例次数', '中心静脉插管使用天数'],
    '呼吸机相关肺炎发病率': ['呼吸机相关肺炎感染例次数', '呼吸机使用天数'],
    '导尿管相关泌尿道感染发病率': ['导尿管相关泌尿道感染例次数', '导尿管使用天数']
}

# 定义原始指标到输出名称的映射关系（支持一对多映射）
INDICATOR_NAME_MAPPING = {
    # 键：原始指标名，值：映射后的输出名称列表
    "新发感染人数": ["医院感染新发病例数", "同期应报告医院感染病例总数"],  # 一对多映射
    "新发感染例次数": ["医院感染新发例次数"],
    "同期住院患者人数": ["同期住院患者人数"],  # 来自汇总表
    "现患率-同期住院患者人数": ["现患率-同期住院患者总数"],  # 来自现患率表
    "感染人数": ["确定时段或时点医院感染患者数"],  # 特殊映射
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

# 严格按要求的输出顺序定义指标名称
RESULT_ROW_ORDER = [
    "医院感染新发病例数",  # 来自新发感染人数
    "医院感染新发例次数",  # 来自新发感染例次数
    "同期住院患者人数",  # 来自汇总表
    "确定时段或时点医院感染患者数",  # 来自现患率表的感染人数
    "确定时段或时点医院感染例次数",  # 来自现患率表的感染例次数
    "现患率-同期住院患者总数",  # 来自现患率表的特殊指标
    "应当报告而未报告的医院感染病例数",  # 来自漏报病例数
    "同期应报告医院感染病例总数",  # 来自新发感染人数的第二个映射
    "受调查的医务人员实际实施手卫生次数",  # 来自手卫生数据
    "同期调查中应实施手卫生次数",  # 来自手卫生数据
    "发生I类切口手术部位感染病例数",  # 来自手术数据
    "同期接受I类切口手术患者总数",  # 来自手术数据
    "血管内导管相关血流感染例次数",  # 来自导管数据
    "同期患者使用血管内导管留置总天数",  # 来自导管数据
    "呼吸机相关肺炎例次数",  # 来自呼吸机数据
    "同期患者使用呼吸机总天数",  # 来自呼吸机数据
    "导尿管相关泌尿系感染例次数",  # 来自导尿管数据
    "同期患者使用导尿管总天数"  # 来自导尿管数据
]


# ================= 配置区域结束 =================

def find_excel_files():
    """
    查找当前目录下所有符合配置规则的Excel文件
    返回:
        dict: 键为文件类型名，值为匹配的文件路径列表
    """
    excel_files = {}
    # 遍历所有文件类型配置
    for file_type, pattern in FILE_PATTERNS.items():
        # 使用glob查找所有xls/xlsx文件，并通过正则表达式筛选
        matched_files = [f for f in glob.glob('*.xls*')
                         if re.search(pattern, f, re.IGNORECASE)
                         and not f.startswith(' 汇总')]  # 排除汇总文件
        if matched_files:
            excel_files[file_type] = matched_files
    return excel_files


def find_header_row(df, file_type):
    """
    自动识别数据表的表头行
    参数:
        df: 原始DataFrame
        file_type: 当前处理的文件类型
    返回:
        int: 表头所在的行号
    """
    # 获取当前文件类型需要查找的指标
    target_indicators = INDICATOR_MAP.get(file_type, [])

    # 遍历每一行，查找包含目标指标的行
    for i, row in df.iterrows():
        # 将行数据拼接为字符串便于查找
        row_str = '|'.join([str(x) for x in row if pd.notna(x)])
        # 检查是否包含任一目标指标
        if any(indicator in row_str for indicator in target_indicators):
            return i  # 返回表头行号

    return 0  # 默认返回第一行


def clean_dataframe(df, file_type):
    """
    清洗原始数据表：设置表头、去除空值、整理列顺序
    参数:
        df: 原始DataFrame
        file_type: 当前处理的文件类型
    返回:
        DataFrame: 清洗后的数据表
    """
    # 查找表头行并设置列名
    header_row = find_header_row(df, file_type)
    df.columns = df.iloc[header_row]  # 设置列名
    df = df[header_row + 1:]  # 去除表头以上的行

    # 特殊处理现患率表的列名
    if file_type == '医院感染现患率':
        df.columns = [col.replace('同期住院患者人数', '现患率-同期住院患者人数')
                      if '同期住院患者人数' in str(col) else col
                      for col in df.columns]

        # 去除全空的行和列
    df = df.dropna(how='all').dropna(how='all', axis=1)
    df = df.reset_index(drop=True)  # 重置索引

    # 确保第一列是科室/全院列
    first_col = df.columns[0]
    if not any(x in str(first_col) for x in ['科室', '全院', '合计']):
        for col in df.columns:
            if any(x in str(col) for x in ['科室', '全院', '合计']):
                # 将识别到的列移动到第一列
                cols = df.columns.tolist()
                cols.insert(0, cols.pop(cols.index(col)))
                df = df[cols]
                break
    return df


def find_quanyuan_row(df):
    """
    查找包含"全院"数据的行
    参数:
        df: 清洗后的DataFrame
    返回:
        Series: 包含全院数据的行
    """
    first_col = df.columns[0]  # 第一列应为科室列

    # 定义可能的全院行标识
    patterns = ['全院', '合计', '总计', '汇总', '全院合计']

    # 尝试各种匹配模式
    for pattern in patterns:
        matches = df[df[first_col].astype(str).str.contains(pattern, na=False)]
        if not matches.empty:
            return matches.iloc[0]  # 返回第一个匹配行

    # 如果只有一行数据，默认就是全院数据
    if df.shape[0] == 1:
        return df.iloc[0]

    return None  # 未找到


def extract_data_from_file(filepath, file_type):
    """
    从单个Excel文件中提取所需指标数据
    参数:
        filepath: 文件路径
        file_type: 文件类型
    返回:
        dict: 提取的指标数据（使用映射后的名称）
    """
    try:
        filename = os.path.basename(filepath)
        print(f"\n处理文件: {filename} (类型: {file_type})")

        # 尝试读取Excel文件（兼容不同引擎）
        try:
            df = pd.read_excel(filepath, header=None)
        except:
            df = pd.read_excel(filepath, header=None, engine='openpyxl')

        print("原始数据前3行:")
        print(df.head(3))  # 打印前3行用于调试

        # 清洗数据
        df = clean_dataframe(df, file_type)
        print("\n清洗后数据前3行:")
        print(df.head(3))

        # 检查数据是否为空
        if df.empty:
            print("警告：清理后表格为空")
            return None

            # 查找全院数据行
        row = find_quanyuan_row(df)
        if row is None:
            print(f"警告：未找到全院行。首列内容:\n{df[df.columns[0]].unique()}")
            return None

        print(f"\n找到全院行数据:\n{row}")

        # 准备提取指标数据
        indicators = INDICATOR_MAP.get(file_type, [])
        data = {}

        # 遍历所有列查找目标指标
        for col in df.columns:
            col_str = str(col).strip()  # 列名转为字符串
            for indicator in indicators:
                if indicator in col_str:  # 检查是否包含目标指标
                    try:
                        value = row[col]  # 获取单元格值

                        # 处理空值和数值类型
                        if pd.isna(value):
                            continue
                        if isinstance(value, (np.int64, np.int32, np.float64)):
                            value = float(value)  # 统一转为浮点数

                        # 应用名称映射
                        if indicator in INDICATOR_NAME_MAPPING:
                            for new_name in INDICATOR_NAME_MAPPING[indicator]:
                                data[new_name] = value
                                print(f"提取成功: {indicator} → {new_name}: {value}")
                        else:
                            data[indicator] = value
                            print(f"提取成功: {indicator}: {value}")
                        break
                    except Exception as e:
                        print(f"提取失败 {indicator}: {str(e)}")

        # 检查是否提取到数据
        if not data:
            print("警告：未提取到任何数据")
            print("可用列:", list(df.columns))

        return data

    except Exception as e:
        print(f"处理文件异常: {str(e)}")
        return None


def main():
    """主程序流程控制"""
    print("开始处理Excel文件...")

    # 查找所有匹配的Excel文件
    excel_files = find_excel_files()
    if not excel_files:
        print("未找到匹配的Excel文件")
        return

        # 打印找到的文件列表
    print("\n找到的文件:")
    for k, v in excel_files.items():
        print(f"{k}: {v}")

    # 准备存储所有提取的数据
    combined_data = {}

    # 逐个处理每个文件
    for file_type, files in excel_files.items():
        for file in files:
            if data := extract_data_from_file(file, file_type):
                combined_data.update(data)  # 合并数据

    # 检查是否提取到数据
    if not combined_data:
        print("\n警告：未提取到任何数据")
        return

        # 创建结果DataFrame
    result = pd.DataFrame(combined_data, index=["全院"]).T
    result.columns = ["全院"]

    # 按严格顺序组织结果
    final_result = pd.DataFrame()
    for indicator in RESULT_ROW_ORDER:
        if indicator in combined_data:
            # 逐个添加指标，确保顺序正确
            final_result = pd.concat([final_result, result.loc[[indicator]]])

            # 保存为Excel文件
    output_path = "汇总.xlsx"
    final_result.to_excel(output_path, index=True)

    # 打印最终结果
    print(f"\n最终汇总结果（按指定顺序）:\n{final_result}")
    print(f"\n成功！结果已保存到: {os.path.abspath(output_path)}")


# 程序入口
if __name__ == "__main__":
    main()