# -*- coding: utf-8 -*-
"""
Excel数据提取脚本（改进版）
功能：增强"全院"行识别能力，确保完整提取所有指标
"""

import pandas as pd
import os
import glob
import re
import numpy as np

# ================= 配置区域 =================
FILE_PATTERNS = {
    '医院感染汇总表': r'医院感染汇总表[^\.]*\.(xlsx|xls)$',
    '医院感染现患率': r'医院感染现患率[^\.]*\.(xlsx|xls)$',
    '手卫生依从正确率': r'手卫生依从正确率[^\.]*\.(xlsx|xls)$',
    'I类切口手术部位感染率': r'I类切口手术部位感染率[^\.]*\.(xlsx|xls)$',
    '血管导管相关血流感染发病率': r'血管导管相关血流感染发病率[^\.]*\.(xlsx|xls)$',
    '呼吸机相关肺炎发病率': r'呼吸机相关肺炎发病率[^\.]*\.(xlsx|xls)$',
    '导尿管相关泌尿道感染发病率': r'导尿管相关泌尿道感染发病率[^\.]*\.(xlsx|xls)$',
}

INDICATOR_MAP = {
    '医院感染汇总表': ['新发感染人数', '新发感染例次数', '同期住院患者人数', '漏报病例数'],
    '医院感染现患率': ['感染人数', '感染例次数'],
    '手卫生依从正确率': ['实际实施手卫生次数', '应实施手卫生次数'],
    'I类切口手术部位感染率': ['Ⅰ类手术部位感染例次数', 'Ⅰ类手术例数'],
    '血管导管相关血流感染发病率': ['血管导管相关血流感染例次数', '中心静脉插管使用天数'],
    '呼吸机相关肺炎发病率': ['呼吸机相关肺炎感染例次数', '呼吸机使用天数'],
    '导尿管相关泌尿道感染发病率': ['导尿管相关泌尿道感染例次数', '导尿管使用天数']
}

INDICATOR_NAME_MAPPING = {
    "新发感染人数": ["医院感染新发病例数", "同期应报告医院感染病例总数"],
    "新发感染例次数": ["医院感染新发例次数"],
    "同期住院患者人数": ["同期住院患者人数"],
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

RESULT_ROW_ORDER = [
    "医院感染新发病例数",
    "医院感染新发例次数",
    "同期住院患者人数",
    "确定时段或时点医院感染患者数",
    "确定时段或时点医院感染例次数",
    "应当报告而未报告的医院感染病例数",
    "同期应报告医院感染病例总数",
    "受调查的医务人员实际实施手卫生次数",
    "同期调查中应实施手卫生次数",
    "发生I类切口手术部位感染病例数",
    "同期接受I类切口手术患者总数",
    "血管内导管相关血流感染例次数",
    "同期患者使用血管内导管留置总天数",
    "呼吸机相关肺炎例次数",
    "同期患者使用呼吸机总天数",
    "导尿管相关泌尿系感染例次数",
    "同期患者使用导尿管总天数"
]


# ================= 配置结束 =================

def find_excel_files():
    """查找匹配的Excel文件"""
    excel_files = {}
    for file_type, pattern in FILE_PATTERNS.items():
        matched_files = [f for f in glob.glob('*.xls*')
                         if re.search(pattern, f, re.IGNORECASE)
                         and not f.startswith(' 汇总')]
        if matched_files:
            excel_files[file_type] = matched_files
    return excel_files


def find_header_row(df, file_type):
    """改进的表头行查找"""
    target_indicators = INDICATOR_MAP.get(file_type, [])
    for i, row in df.iterrows():
        row_str = '|'.join([str(x) for x in row if pd.notna(x)])
        if any(indicator in row_str for indicator in target_indicators):
            return i
    return 0


def clean_dataframe(df, file_type):
    """改进的数据清洗"""
    header_row = find_header_row(df, file_type)
    df.columns = df.iloc[header_row]
    df = df[header_row + 1:]

    # 改进的空值处理
    df = df.dropna(how='all').dropna(how='all', axis=1)
    df = df.reset_index(drop=True)

    # 改进的科室列识别
    first_col = df.columns[0]
    if not any(x in str(first_col) for x in ['科室', '全院', '合计']):
        for col in df.columns:
            if any(x in str(col) for x in ['科室', '全院', '合计']):
                cols = df.columns.tolist()
                cols.insert(0, cols.pop(cols.index(col)))
                df = df[cols]
                break
    return df


def find_quanyuan_row(df):
    """改进的全院行查找"""
    first_col = df.columns[0]

    # 尝试多种匹配方式
    patterns = ['全院', '合计', '总计', '汇总', '全院合计']
    for pattern in patterns:
        matches = df[df[first_col].astype(str).str.contains(pattern, na=False)]
        if not matches.empty:
            return matches.iloc[0]

            # 检查是否首列本身就是全院数据
    if df.shape[0] == 1:
        return df.iloc[0]

    return None


def extract_data_from_file(filepath, file_type):
    """改进的数据提取"""
    try:
        filename = os.path.basename(filepath)
        print(f"\n处理文件: {filename} (类型: {file_type})")

        # 读取时尝试多种引擎
        try:
            df = pd.read_excel(filepath, header=None)
        except:
            df = pd.read_excel(filepath, header=None, engine='openpyxl')

        print("原始数据前3行:")
        print(df.head(3))

        df = clean_dataframe(df, file_type)
        print("\n清洗后数据前3行:")
        print(df.head(3))

        if df.empty:
            print("警告：清理后表格为空")
            return None

            # 改进的全院行查找
        row = find_quanyuan_row(df)
        if row is None:
            print(f"警告：未找到全院行。首列内容:\n{df[df.columns[0]].unique()}")
            return None

        print(f"\n找到全院行数据:\n{row}")

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

                        # 名称映射
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

        if not data:
            print("警告：未提取到任何数据")
            print("可用列:", list(df.columns))

        return data
    except Exception as e:
        print(f"处理文件异常: {str(e)}")
        return None


def main():
    """主程序"""
    print("开始处理Excel文件...")
    excel_files = find_excel_files()
    if not excel_files:
        print("未找到匹配的Excel文件")
        return

    print("\n找到的文件:")
    for k, v in excel_files.items():
        print(f"{k}: {v}")

    combined_data = {}
    for file_type, files in excel_files.items():
        for file in files:
            if data := extract_data_from_file(file, file_type):
                combined_data.update(data)

    if not combined_data:
        print("\n警告：未提取到任何数据")
        return

        # 生成结果
    result = pd.DataFrame(combined_data, index=["全院"]).T
    result.columns = ["全院"]

    # 按顺序排列
    available_rows = [r for r in RESULT_ROW_ORDER if r in combined_data]
    result = result.loc[available_rows]

    # 保存结果
    output_path = "汇总.xlsx"
    result.to_excel(output_path, index=True)

    print(f"\n汇总结果:\n{result}")
    print(f"\n成功！结果已保存到: {os.path.abspath(output_path)}")


if __name__ == "__main__":
    main()