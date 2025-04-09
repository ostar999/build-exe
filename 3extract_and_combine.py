# -*- coding: utf-8 -*-
"""
Excel数据提取脚本
功能：从多个Excel文件中提取指定指标数据，并汇总到一个文件中
"""

# 导入必要的库
import pandas as pd
import os
import glob
import re
import numpy as np

# ================= 配置区域 =================

# 文件匹配规则
FILE_PATTERNS = {
    '医院感染汇总表': r'医院感染汇总表[^\.]*\.xlsx$',
    '医院感染现患率': r'医院感染现患率[^\.]*\.xlsx$',
    '手卫生依从正确率': r'手卫生依从正确率[^\.]*\.(xlsx|xls)$',  # 支持 .xls 结尾
    'I类切口手术部位感染率': r'I类切口手术部位感染率[^\.]*\.xlsx$',
    '血管导管相关血流感染发病率': r'血管导管相关血流感染发病率[^\.]*\.xlsx$',
    '呼吸机相关肺炎发病率': r'呼吸机相关肺炎发病率[^\.]*\.xlsx$',
    '导尿管相关泌尿道感染发病率': r'导尿管相关泌尿道感染发病率[^\.]*\.xlsx$',
}

# 指标名称映射
INDICATOR_MAP = {
    '医院感染汇总表': ['新发感染人数', '新发感染例次数', '同期住院患者人数', '漏报病例数'],
    '医院感染现患率': ['感染人数', '感染例次数'],
    '手卫生依从正确率': ['实际实施手卫生次数', '应实施手卫生次数'],
    'I类切口手术部位感染率': ['Ⅰ类手术部位感染例次数', 'Ⅰ类手术例数'],
    '血管导管相关血流感染发病率': ['血管导管相关血流感染例次数', '中心静脉插管使用天数'],
    '呼吸机相关肺炎发病率': ['呼吸机相关肺炎感染例次数', '呼吸机使用天数'],
    '导尿管相关泌尿道感染发病率': ['导尿管相关泌尿道感染例次数', '导尿管使用天数']
}

# 汇总表中指标显示顺序
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
    """查找当前目录下所有匹配的Excel文件"""
    excel_files = {}
    for file_type, pattern in FILE_PATTERNS.items():
        matched_files = [f for f in glob.glob('*.xls*')
                         if re.search(pattern, f, re.IGNORECASE)
                         and not f.startswith('汇总')]
        if matched_files:
            excel_files[file_type] = matched_files
    return excel_files


def find_header_row(df, file_type):
    """查找表头所在行"""
    target_indicators = INDICATOR_MAP.get(file_type, [])
    for i, row in df.iterrows():
        row_str = '|'.join([str(x) for x in row if pd.notna(x)])
        if any(indicator in row_str for indicator in target_indicators):
            return i
    return 0


def clean_dataframe(df, file_type):
    """清理DataFrame，设置表头，确保首列为科室信息"""
    header_row = find_header_row(df, file_type)
    df.columns = df.iloc[header_row]
    df = df[header_row + 1:]
    df = df.dropna(how='all').dropna(how='all', axis=1)
    df = df.reset_index(drop=True)

    if len(df.columns) > 0 and not any(x in str(df.columns[0]) for x in ['科室', '全院', '合计']):
        for col in df.columns:
            if any(x in str(col) for x in ['科室', '全院', '合计']):
                cols = df.columns.tolist()
                cols.insert(0, cols.pop(cols.index(col)))
                df = df[cols]
                break
    return df


def extract_data_from_file(filepath, file_type):
    """从单个Excel文件提取数据"""
    try:
        filename = os.path.basename(filepath)
        print(f"\n处理文件: {filename} (类型: {file_type})")

        df = pd.read_excel(filepath, header=None)
        print("原始内容预览:")
        print(df.head(3))

        df = clean_dataframe(df, file_type)
        print("清理后的内容预览:")
        print(df.head(3))

        if df.empty:
            print("警告：清理后表格为空")
            return None

        first_col = df.columns[0]

        # 修改这里：支持"全院"或"合计"
        quan_yuan_row = df[df[first_col].astype(str).str.strip().isin(["全院", "合计"])]
        if quan_yuan_row.empty:
            print(f"警告：未找到'全院'或'合计'行。首列内容:\n{df[first_col].unique()}")
            return None

        row = quan_yuan_row.iloc[0]
        print(f"找到指标数据行: {row.values}")

        indicators = INDICATOR_MAP.get(file_type, [])
        data = {}
        for col in df.columns:
            col_str = str(col).strip()
            for indicator in indicators:
                if indicator in col_str:
                    try:
                        value = row[col]
                        if pd.isna(value):
                            continue
                        if isinstance(value, (np.int64, np.int32, np.float64)):
                            value = float(value)
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
    """主函数入口"""
    print("开始处理Excel文件...")
    excel_files = find_excel_files()
    if not excel_files:
        print("未找到任何匹配的Excel文件")
        return

    print("\n找到的文件列表：")
    for k, v in excel_files.items():
        print(f"{k}: {v}")

    combined_data = {}
    for file_type, files in excel_files.items():
        for file in files:
            if data := extract_data_from_file(file, file_type):
                combined_data.update(data)

    if not combined_data:
        print("未提取到任何数据")
        return

    result = pd.DataFrame(combined_data, index=["全院"]).T
    result.columns = ["全院"]

    available_rows = [r for r in RESULT_ROW_ORDER if r in combined_data]
    result = result.loc[available_rows]

    output_path = "汇总.xlsx"
    result.to_excel(output_path, index=True)
    print(f"\n成功！数据汇总已保存：{os.path.abspath(output_path)}")
    print(result)


if __name__ == "__main__":
    main()
