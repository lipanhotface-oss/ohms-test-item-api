# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as ET
from time import sleep
from xmldiff import main, formatting
import pandas as pd
from tqdm import tqdm  # ✅ 新增：进度条库

def get_all_xml_files(folder):
    """获取文件夹内的所有 XML 文件路径"""
    xml_files = {}
    for root, _, files in os.walk(folder):
        for file in files:
            if file.lower().endswith('.xml'):
                xml_files[file] = os.path.join(root, file)
    return xml_files


def flatten_xml(root, parent_path=""):
    """将 XML 展平为 [(xpath, 属性dict, 文本)]，为同级同名节点加编号"""
    nodes = []
    tag_count = {}
    for child in root:
        tag_count[child.tag] = tag_count.get(child.tag, 0) + 1
        index = tag_count[child.tag]
        current_path = f"{parent_path}/{child.tag}[{index}]" if parent_path else f"{child.tag}[{index}]"

        nodes.append((current_path, dict(sorted(child.attrib.items())), (child.text or "").strip()))
        nodes.extend(flatten_xml(child, current_path))
    return nodes


def compare_nodes_detail(nodes_a, nodes_b, file_a_name, file_b_name):
    """详细对比两个 XML 节点，输出每条差异（带源/对比文件与值）"""
    diffs = []

    dict_a = {n[0]: (n[1], n[2]) for n in nodes_a}
    dict_b = {n[0]: (n[1], n[2]) for n in nodes_b}

    # 所有路径集合
    all_paths = set(dict_a.keys()) | set(dict_b.keys())

    for path in sorted(all_paths):
        attrs_a, text_a = dict_a.get(path, ({}, ""))
        attrs_b, text_b = dict_b.get(path, ({}, ""))

        # 节点新增 / 删除
        if path not in dict_a:
            diffs.append([
                file_a_name, path, "节点新增",
                file_a_name, "", file_b_name, "新增节点"
            ])
            continue
        elif path not in dict_b:
            diffs.append([
                file_a_name, path, "节点删除",
                file_a_name, "存在", file_b_name, "已删除"
            ])
            continue

        # 属性差异
        all_keys = set(attrs_a.keys()) | set(attrs_b.keys())
        for key in sorted(all_keys):
            val_a = attrs_a.get(key)
            val_b = attrs_b.get(key)
            if val_a != val_b:
                diffs.append([
                    file_a_name, f"{path}/@{key}", "属性差异",
                    file_a_name, val_a, file_b_name, val_b
                ])

        # 文本差异
        if text_a != text_b:
            diffs.append([
                file_a_name, f"{path}", "文本差异",
                file_a_name, text_a, file_b_name, text_b
            ])

    return diffs


def conpare_xml2(file_a, ile_b):
    diff = main.diff_files(file_a, ile_b)
    formatter=formatting.XmlDiffFormatter()
    if diff:
        print(diff)
    return diff

def compare_xml(file_a, file_b):
    """对比两个 XML 文件"""
    try:
        root_a = ET.parse(file_a).getroot()
        root_b = ET.parse(file_b).getroot()
    except Exception as e:
        return {"error": f"解析失败: {e}"}

    nodes_a = flatten_xml(root_a)
    nodes_b = flatten_xml(root_b)

    return compare_nodes_detail(nodes_a, nodes_b, os.path.basename(file_a), os.path.basename(file_b))


def main1(folder_a, folder_b, output_excel="xml_diff_result.xlsx"):
    files_a = get_all_xml_files(folder_a)
    files_b = get_all_xml_files(folder_b)

    all_diffs = []
    all_names = sorted(set(files_a.keys()) | set(files_b.keys()))

    # ✅ 用 tqdm 包裹循环，显示进度条
    for name in tqdm(all_names, desc="对比进度", unit="文件",ncols=120):
        # sleep(0.1)
        if name not in files_a:
            all_diffs.append([name, "(无匹配文件)", "", folder_a, "", folder_b, "仅存在于B"])
            continue
        if name not in files_b:
            all_diffs.append([name, "(无匹配文件)", "", folder_a, "仅存在于A", folder_b, ""])
            continue

        diff = conpare_xml2(files_a[name], files_b[name])
        
        # if isinstance(diff, dict) and "error" in diff:
        #     all_diffs.append([name, "(解析错误)", diff["error"], folder_a, "", folder_b, ""])
        #     continue

        # if diff:
        #     all_diffs.extend(diff)
        # else:
        #     all_diffs.append([name, "(一致)", "", folder_a, "", folder_b, ""])

    # 输出 Excel
    # df = pd.DataFrame(all_diffs, columns=[
    #     "文件名", "节点路径/属性", "差异类型",
    #     "源文件", "源文件值", "对比文件", "对比文件值"
    # ])
    # df.to_excel(output_excel, index=False)
    # print(f"\n✅ 对比完成，结果已保存至：{output_excel}")


if __name__ == "__main__":
    folder_a = r"D:\ahmu\文件\[公开] ICD CXF AS2.0_CFG1.1 B版\CXF ICD CXF AS2.0_CFG1.1 B版\Model System Elements"
    folder_b = r"D:\ahmu\文件\[公开] ICD CXF AS2.0_CFG1.3\CXF ICD CXF AS2.0_CFG1.3\Model System Elements"
    main1(folder_a, folder_b)
