import pandas as pd
import argparse
from typing import Tuple, Set, Literal, List, Dict
import os

def load_source_xlsx(xlsx_path: str) -> Set[str]:
    """
    加载XLSX源文件，提取 Word_Name/Message_Name 列的不重复值集合
    """
    try:
        xlsx_df = pd.read_excel(xlsx_path, engine='openpyxl', sheet_name="BUS")
        if 'Word_Name/Message_Name' not in xlsx_df.columns:
            raise ValueError("XLSX文件中未找到 'Word_Name/Message_Name' 列")
        
        # 数据清洗：去重、去空值、转为字符串
        source_values = xlsx_df['Word_Name/Message_Name'].dropna().astype(str).unique()
        source_set = set(source_values)
        print(f"✅ XLSX源文件加载完成：共 {len(source_set)} 个不重复的 Word_Name/Message_Name")
        return source_set
    except FileNotFoundError as e:
        print(f"❌ 错误：XLSX文件未找到 - {e}")
        exit(1)
    except Exception as e:
        print(f"❌ XLSX数据加载错误：{e}")
        exit(1)

def load_target_csvs(csv1_path: str, csv1_col: str, csv2_path: str, csv2_col: str) -> Tuple[Set[str], Set[str], str, str]:
    """
    加载两个CSV目标文件，提取各自指定列的不重复值集合，同时返回CSV文件名（用于标注来源）
    Returns: (csv1_set, csv2_set, csv1_filename, csv2_filename)
    """
    def load_single_csv(csv_path: str, col_name: str, csv_label: str) -> Tuple[Set[str], str]:
        """辅助函数：加载单个CSV的指定列，返回值集合和文件名"""
        try:
            csv_df = pd.read_csv(csv_path)
            if col_name not in csv_df.columns:
                raise ValueError(f"未找到列 '{col_name}'")
            
            csv_values = csv_df[col_name].dropna().astype(str).unique()
            csv_set = set(csv_values)
            csv_filename = os.path.basename(csv_path)  # 提取文件名（不含路径）
            print(f"✅ {csv_label}（CSV）加载完成：共 {len(csv_set)} 个不重复的 '{col_name}'（文件：{csv_filename}）")
            return csv_set, csv_filename
        except FileNotFoundError as e:
            print(f"❌ 错误：{csv_label}未找到 - {e}")
            exit(1)
        except Exception as e:
            print(f"❌ {csv_label}数据加载错误：{e}")
            exit(1)
    
    # 加载两个CSV，获取值集合和文件名
    csv1_set, csv1_filename = load_single_csv(csv1_path, csv1_col, "第一个目标CSV")
    csv2_set, csv2_filename = load_single_csv(csv2_path, csv2_col, "第二个目标CSV")
    return csv1_set, csv2_set, csv1_filename, csv2_filename

def compare_data(
    source_set: Set[str],
    csv1_set: Set[str],
    csv2_set: Set[str],
    csv1_filename: str,
    csv2_filename: str
) -> List[Dict[str, str]]:
    """
    对比源数据在两个CSV中的存在情况，返回详细结果列表（含来源标注）
    Returns: 列表包含字典，每个字典对应一个MESSAGE_NAME的对比结果
    """
    result_list = []
    for message in sorted(source_set):  # 按字母排序
        in_csv1 = message in csv1_set
        in_csv2 = message in csv2_set
        
        # 标注来源
        source_labels = []
        if in_csv1:
            source_labels.append(csv1_filename)
        if in_csv2:
            source_labels.append(csv2_filename)
        
        source_desc = "、".join(source_labels) if source_labels else "无"
        status = "两个CSV都存在" if in_csv1 and in_csv2 else \
                "只在第一个CSV存在" if in_csv1 else \
                "只在第二个CSV存在" if in_csv2 else \
                "两个CSV都不存在"
        
        result_list.append({
            "Word_Name/Message_Name": message,
            "存在状态": status,
            "查找来源（CSV文件）": source_desc
        })
    return result_list

def generate_txt_report(
    result_list: List[Dict[str, str]],
    csv1_col: str,
    csv2_col: str,
    csv1_filename: str,
    csv2_filename: str,
    output_path: str = "XLSX_双CSV对比结果.txt"
):
    """生成详细TXT报告（含来源标注）"""
    total_source = len(result_list)
    both_exists = sum(1 for item in result_list if item["存在状态"] == "两个CSV都存在")
    only_csv1 = sum(1 for item in result_list if item["存在状态"] == "只在第一个CSV存在")
    only_csv2 = sum(1 for item in result_list if item["存在状态"] == "只在第二个CSV存在")
    neither = sum(1 for item in result_list if item["存在状态"] == "两个CSV都不存在")
    
    # 计算占比
    both_rate = (both_exists / total_source) * 100 if total_source > 0 else 0
    only_csv1_rate = (only_csv1 / total_source) * 100 if total_source > 0 else 0
    only_csv2_rate = (only_csv2 / total_source) * 100 if total_source > 0 else 0
    neither_rate = (neither / total_source) * 100 if total_source > 0 else 0
    
    # 生成报告内容
    report = f"""
==================================== 对比报告 ====================================
📋 对比配置：
- 源数据：XLSX文件的 'Word_Name/Message_Name' 列
- 目标数据1：CSV文件 '{csv1_filename}' 的 '{csv1_col}' 列
- 目标数据2：CSV文件 '{csv2_filename}' 的 '{csv2_col}' 列

📊 统计信息：
- 源数据总不重复值：{total_source} 个
- ✅ 两个CSV都存在：{both_exists} 个（{both_rate:.2f}%）
- ⚠️  只在 '{csv1_filename}' 存在：{only_csv1} 个（{only_csv1_rate:.2f}%）
- ⚠️  只在 '{csv2_filename}' 存在：{only_csv2} 个（{only_csv2_rate:.2f}%）
- ❌ 两个CSV都不存在：{neither} 个（{neither_rate:.2f}%）

--------------------------------------------------------------------------------
📋 详细结果列表（按Word_Name/Message_Name排序）：
"""
    
    # 添加每条结果的详细信息
    for idx, item in enumerate(result_list, 1):
        report += f"\n{idx:03d}. 名称：{item['Word_Name/Message_Name']}"
        report += f"\n   状态：{item['存在状态']}"
        report += f"\n   来源：{item['查找来源（CSV文件）']}"
        report += "\n" + "-"*80
    
    report += f"""
================================================================================
📝 说明：
- 查找来源标注了该名称在哪些CSV文件中被找到（多个文件用、分隔）
- 所有结果已按 Word_Name/Message_Name 字母顺序排序
================================================================================
    """
    
    # 保存TXT报告
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n📄 TXT报告已保存到：{os.path.abspath(output_path)}")

def generate_xlsx_report(
    result_list: List[Dict[str, str]],
    csv1_col: str,
    csv2_col: str,
    csv1_filename: str,
    csv2_filename: str,
    output_path: str = "XLSX_双CSV对比结果.xlsx"
):
    """生成XLSX格式报告（含来源标注，便于后续筛选处理）"""
    # 转换为DataFrame
    df = pd.DataFrame(result_list)
    
    # 创建Excel写入器，设置样式（可选，让表格更美观）
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 写入详细结果表
        df.to_excel(writer, sheet_name="详细对比结果", index=False)
        
        # 写入统计汇总表
        total_source = len(result_list)
        summary_data = [
            ["统计项", "数量", "占比（%）"],
            ["源数据总不重复值", total_source, "100.00"],
            ["两个CSV都存在", sum(1 for item in result_list if item["存在状态"] == "两个CSV都存在"), 
             f"{(sum(1 for item in result_list if item['存在状态'] == '两个CSV都存在')/total_source*100):.2f}" if total_source>0 else "0.00"],
            [f"只在 '{csv1_filename}' 存在", sum(1 for item in result_list if item["存在状态"] == "只在第一个CSV存在"),
             f"{(sum(1 for item in result_list if item['存在状态'] == '只在第一个CSV存在')/total_source*100):.2f}" if total_source>0 else "0.00"],
            [f"只在 '{csv2_filename}' 存在", sum(1 for item in result_list if item["存在状态"] == "只在第二个CSV存在"),
             f"{(sum(1 for item in result_list if item['存在状态'] == '只在第二个CSV存在')/total_source*100):.2f}" if total_source>0 else "0.00"],
            ["两个CSV都不存在", sum(1 for item in result_list if item["存在状态"] == "两个CSV都不存在"),
             f"{(sum(1 for item in result_list if item['存在状态'] == '两个CSV都不存在')/total_source*100):.2f}" if total_source>0 else "0.00"]
        ]
        summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
        summary_df.to_excel(writer, sheet_name="统计汇总", index=False)
        
        # 写入配置信息表
        config_data = [
            ["配置项", "内容"],
            ["源数据文件", os.path.abspath(args.xlsx)],
            ["源数据列名", "Word_Name/Message_Name"],
            ["第一个CSV文件", f"{csv1_filename}（列名：{csv1_col}）"],
            ["第二个CSV文件", f"{csv2_filename}（列名：{csv2_col}）"],
            ["对比时间", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")]
        ]
        config_df = pd.DataFrame(config_data[1:], columns=config_data[0])
        config_df.to_excel(writer, sheet_name="对比配置", index=False)
    
    print(f"📊 XLSX报告已保存到：{os.path.abspath(output_path)}")

def main():
    parser = argparse.ArgumentParser(description="XLSX的Word_Name/Message_Name列，在两个CSV的指定列中查找存在性（输出TXT+XLSX）")
    parser.add_argument("--xlsx", required=True, help="XLSX源文件路径（含Word_Name/Message_Name列）")
    parser.add_argument("--csv1", required=True, help="第一个CSV目标文件路径")
    parser.add_argument("--csv1-col", required=True, help="第一个CSV中用于查找的列名")
    parser.add_argument("--csv2", required=True, help="第二个CSV目标文件路径")
    parser.add_argument("--csv2-col", required=True, help="第二个CSV中用于查找的列名")
    parser.add_argument("--txt-output", default="XLSX_双CSV对比结果.txt", help="TXT报告输出路径（默认：XLSX_双CSV对比结果.txt）")
    parser.add_argument("--xlsx-output", default="XLSX_双CSV对比结果.xlsx", help="XLSX报告输出路径（默认：XLSX_双CSV对比结果.xlsx）")
    
    global args  # 全局变量，供generate_xlsx_report使用
    args = parser.parse_args()
    
    # 1. 加载所有数据（含CSV文件名提取）
    source_set = load_source_xlsx(args.xlsx)
    csv1_set, csv2_set, csv1_filename, csv2_filename = load_target_csvs(
        args.csv1, args.csv1_col, args.csv2, args.csv2_col
    )
    
    # 2. 对比数据（生成详细结果列表，含来源标注）
    result_list = compare_data(source_set, csv1_set, csv2_set, csv1_filename, csv2_filename)
    
    # 3. 生成双格式报告
    generate_txt_report(result_list, args.csv1_col, args.csv2_col, csv1_filename, csv2_filename, args.txt_output)
    generate_xlsx_report(result_list, args.csv1_col, args.csv2_col, csv1_filename, csv2_filename, args.xlsx_output)
    
    print("\n🎉 对比完成！已生成TXT和XLSX两种格式报告")

if __name__ == "__main__":
    main()

# python .\compare_csv_and_xlsx_messagename_by_eoicd.py --xlsx EoICD_Subscriber_Table.xlsx --csv1 HA.csv --csv2 HF.csv --csv1-col Word_Name/Message_Name --csv2-col Word_Name/Message_Name