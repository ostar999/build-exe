# -*- coding: utf-8 -*-
"""
Excel数据提取脚本
功能：从多个Excel文件中提取指定指标数据，并汇总到一个文件中
"""

# 导入必要的库
import pandas as pd  # 数据处理核心库
import os  # 操作系统路径处理
import glob  # 文件路径匹配
import re  # 正则表达式
import numpy as np  # 数值计算支持

# ================= 配置区域 =================
# 文件匹配规则（使用正则表达式）
FILE_PATTERNS = {
    '医院感染汇总表': r'医院感染汇总表[^\.]*\.xlsx$',  # 匹配医院感染汇总表开头，后面跟着任意字符直到.xlsx
    '医院感染现患率': r'医院感染现患率[^\.]*\.xlsx$',  # 匹配医院感染现患率开头的文件
    '手卫生依从正确率': r'手卫生依从正确率[^\.]*\.xlsx$',  # 匹配手卫生依从正确率开头的文件
    'I类切口手术部位感染率': r'I类切口手术部位感染率[^\.]*\.xlsx$',  # 匹配I类切口手术部位感染率开头的文件
    '血管导管相关血流感染发病率': r'血管导管相关血流感染发病率[^\.]*\.xlsx$',  # 匹配血管导管相关血流感染发病率开头的文件
    '呼吸机相关肺炎发病率': r'呼吸机相关肺炎发病率[^\.]*\.xlsx$',  # 匹配呼吸机相关肺炎发病率开头的文件
    '导尿管相关泌尿道感染发病率': r'导尿管相关泌尿道感染发病率[^\.]*\.xlsx$',  # 匹配导尿管相关泌尿道感染发病率开头的文件
}

# 指标名称映射（支持部分匹配）
INDICATOR_MAP = {
    '医院感染汇总表': ['新发感染人数', '新发感染例次数', '同期住院患者人数','漏报病例数'],
    '医院感染现患率': ['感染人数', '感染例次数'],
    '手卫生依从正确率': ['实际实施手卫生次数', '应实施手卫生次数'],
    'I类切口手术部位感染率': ['Ⅰ类手术部位感染例次数', 'Ⅰ类手术例数'],
    '血管导管相关血流感染发病率': ['血管导管相关血流感染例次数', '中心静脉插管使用天数'],
    '呼吸机相关肺炎发病率': ['呼吸机相关肺炎感染例次数', '呼吸机使用天数'],
    '导尿管相关泌尿道感染发病率': ['导尿管相关泌尿道感染例次数', '导尿管使用天数']
}


# 汇总表中指标的显示顺序
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
    "导尿管相关泌尿道感染例次数",
    "导尿管使用天数"
]


# ================= 配置结束 =================

def find_excel_files():
    """
    查找当前目录下所有符合模式的Excel文件
    返回:
        dict: 按文件类型分类的文件路径字典
    """
    excel_files = {}

    # 遍历所有文件匹配规则
    for file_type, pattern in FILE_PATTERNS.items():
        # 查找匹配该模式的所有文件（不区分大小写）
        matched_files = [f for f in glob.glob('*.xls') + glob.glob('*.xlsx')
                         if re.search(pattern, f, re.IGNORECASE)
                         and not f.startswith(' 汇总.')]  # 排除汇总文件

        if matched_files:
            excel_files[file_type] = matched_files

    return excel_files


def find_header_row(df, file_type):
    """
    智能查找表头所在行
    参数:
        df: 原始数据框
        file_type: 文件类型（t1/t2等）
    返回:
        int: 表头所在行号
    """
    # 获取该文件类型的目标指标
    target_indicators = INDICATOR_MAP.get(file_type, [])

    # 逐行检查是否包含目标指标
    for i, row in df.iterrows():
        # 将非空值合并为字符串
        row_str = '|'.join([str(x) for x in row if pd.notna(x)])
        # 检查是否包含任何目标指标
        if any(indicator in row_str for indicator in target_indicators):
            return i
    return 0  # 默认返回第一行


def clean_dataframe(df, file_type):
    """
    清理数据框并正确设置表头
    参数:
        df: 原始数据框
        file_type: 文件类型
    返回:
        DataFrame: 清理后的数据框
    """
    # 查找表头行
    header_row = find_header_row(df, file_type)

    # 设置表头
    if header_row >= 0:
        df.columns = df.iloc[header_row]  # 将指定行设为列名
        df = df[header_row + 1:]  # 去除表头行

    # 去除全为空的行和列
    df = df.dropna(how='all').dropna(how='all', axis=1)

    # 重置索引
    df = df.reset_index(drop=True)

    # 确保第一列是科室信息
    if len(df.columns) > 0 and not any(x in str(df.columns[0]) for x in ['科室', '全院']):
        # 查找包含科室信息的列
        for col in df.columns:
            if any(x in str(col) for x in ['科室', '全院']):
                # 将该列设为第一列
                cols = df.columns.tolist()
                cols.insert(0, cols.pop(cols.index(col)))
                df = df[cols]
                break

    return df


def extract_data_from_file(filepath, file_type):
    """
    从Excel文件中提取所需数据
    参数:
        filepath: 文件路径
        file_type: 文件类型
    返回:
        dict: 提取的数据字典
    """
    try:
        filename = os.path.basename(filepath)
        print(f"\n处理文件: {filename} (类型: {file_type})")

        # 读取文件（不自动识别表头）
        df = pd.read_excel(filepath, header=None)
        print("原始文件预览:")
        print(df.head(3))  # 打印前3行

        # 清理数据并设置表头
        df = clean_dataframe(df, file_type)
        print("清理后的表格:")
        print(df.head(3))

        # 检查空数据框
        if df.empty:
            print("警告：清理后表格为空")
            return None

        data = {}
        first_col = df.columns[0]  # 获取第一列名称

        # 查找"全院"行（精确匹配）
        quan_yuan_row = df[df[first_col].astype(str).str.strip() == "全院"]
        if quan_yuan_row.empty:
            print(f"警告：未找到'全院'行。首列内容:\n{df[first_col].unique()}")
            return None

            # 获取全院行数据
        row = quan_yuan_row.iloc[0]
        print(f"找到全院行数据: {row.values}")

        # 获取该文件类型需要提取的指标
        indicators = INDICATOR_MAP.get(file_type, [])

        # 提取数据
        extracted_count = 0
        for col in df.columns:
            col_str = str(col).strip()  # 列名去空格

            # 检查当前列是否匹配任何指标（支持部分匹配）
            for indicator in indicators:
                if indicator in col_str:
                    try:
                        value = row[col]
                        if pd.isna(value):  # 跳过空值
                            continue
                            # 转换numpy数值类型为Python原生类型
                        if isinstance(value, (np.int64, np.int32)):
                            value = int(value)
                        data[indicator] = value
                        print(f"√ 提取指标 '{indicator}': {value}")
                        extracted_count += 1
                        break  # 找到匹配后跳出循环
                    except Exception as e:
                        print(f"× 提取指标 '{indicator}' 失败: {str(e)}")

        if extracted_count == 0:
            print("警告：未提取到任何指标数据")
            print(f"可用列名: {list(df.columns)}")
            return None

        return data

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        return None


def main():
    """主函数"""
    print("开始扫描并处理Excel文件...")
    # 查找所有符合条件的Excel文件
    excel_files = find_excel_files()

    if not excel_files:
        print("错误：未找到任何符合条件的Excel文件")
        return

        # 打印找到的文件
    print("\n找到的文件:")
    for file_type, files in excel_files.items():
        print(f"{file_type.upper()} 类文件: {files}")

    # 提取数据
    combined_data = {}
    for file_type, files in excel_files.items():
        for file in files:
            # 提取数据并合并
            if data := extract_data_from_file(file, file_type):
                combined_data.update(data)

    if not combined_data:
        print("\n错误：未提取到有效数据")
        return

        # 创建并保存汇总表
    result = pd.DataFrame(combined_data, index=["全院"]).T
    result.columns = ["全院"]  # 设置列名

    # 按预定顺序排列行
    available_rows = [r for r in RESULT_ROW_ORDER if r in combined_data]
    result = result.loc[available_rows]

    # 输出文件路径
    output_path = "汇总.xlsx"
    result.to_excel(output_path, index=True, header=True)

    print(f"\n成功！数据已汇总保存到: {os.path.abspath(output_path)}")
    print("\n最终汇总结果:")
    print(result)


if __name__ == "__main__":
    # 脚本入口
    main()