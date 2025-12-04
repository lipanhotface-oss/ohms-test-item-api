#  _*_ coding:utf-8 _*_
import os
import xml.etree.ElementTree as ET
import logging
from logging.handlers import RotatingFileHandler
from tqdm import tqdm
import pandas as pd
import sqlite3
from datetime import datetime
from collections import defaultdict


# -------------------------- 清空文件内容（保留文件） --------------------------
def clear_file_contents(log_path="excel_xml_check.log", db_path="xml_guid_mapping.db"):
    if os.path.exists(log_path):
        try:
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("")
            logger.debug(f"已清空日志文件：{log_path}")
        except Exception as e:
            logger.warning(f"清空日志失败：{str(e)}")

    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM guid_xml_mapping")
            cursor.execute("DELETE FROM xml_metadata")
            cursor.execute("DELETE FROM excel_metadata")
            conn.commit()
            conn.close()
            logger.debug(f"已清空数据库表数据：{db_path}")
        except Exception as e:
            logger.error(f"清空数据库失败：{str(e)}")
            # os.remove(db_path)
            logger.info(f"重建数据库：{db_path}")
    logger.info("文件内容清空完成")


# -------------------------- 1. 配置日志 --------------------------
def setup_logging():
    logger = logging.getLogger("ExcelToXMLChecker")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        filename="excel_xml_check.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# -------------------------- 2. 数据库初始化（新增xml_file_name字段） --------------------------
def init_database(db_path="xml_guid_mapping.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. GUID-XML映射表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS guid_xml_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guid TEXT NOT NULL,
        xml_file_path TEXT NOT NULL,  -- 关联xml_metadata的完整路径
        match_node_path TEXT NOT NULL,
        match_attribute TEXT NOT NULL
    )
    ''')

    # 2. XML元数据表（新增xml_file_name字段存储纯文件名）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS xml_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        xml_file_path TEXT NOT NULL UNIQUE,  -- 完整路径+文件名（如D:/a/b.xml）
        xml_file_name TEXT NOT NULL,         -- 纯文件名（如b.xml）
        physical_port TEXT,
        message_name TEXT,
        dp_name TEXT,
        full_name TEXT,
        parse_time TIMESTAMP NOT NULL
    )
    ''')

    # 3. Excel元数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS excel_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guid TEXT NOT NULL UNIQUE,
        physical_port TEXT,
        message_name TEXT,
        dp_name TEXT,
        full_name TEXT,
        source_row INTEGER
    )
    ''')

    # 索引（新增xml_file_name索引，加速按文件名查询）
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_guid ON guid_xml_mapping(guid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_xml_path ON guid_xml_mapping(xml_file_path)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_xml_name ON xml_metadata(xml_file_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_excel_guid ON excel_metadata(guid)")

    conn.commit()
    conn.close()
    logger.info(f"数据库初始化完成：{db_path}（含xml_file_name字段）")
    return db_path


# -------------------------- 3. XML元数据提取 --------------------------
def extract_xml_metadata(root):
    metadata = {
        "physical_port": None,
        "message_name": None,
        "dp_name": None,
        "full_name": None
    }
    keyword_mapping = {
        "physical_port": ["physicalport", "physical_port", "port", "phyport"],
        "message_name": ["message", "msg", "word_name", "messagename"],
        "dp_name": ["dpname", "dp_name", "DP_Name"],
        "full_name": ["fullname", "full_name", "全名"]
    }

    def traverse_nodes(node):
        # 检查属性
        for attr_name, attr_value in node.attrib.items():
            attr_name_lower = attr_name.lower()
            attr_value_lower = str(attr_value).lower()
            for field, keywords in keyword_mapping.items():
                if metadata[field] is not None:
                    continue
                for kw in keywords:
                    if kw in attr_name_lower or kw in attr_value_lower:
                        metadata[field] = attr_value
                        break
        # 检查文本
        node_text = str(node.text).strip() if node.text else ""
        if node_text:
            for field, keywords in keyword_mapping.items():
                if metadata[field] is not None:
                    continue
                for kw in keywords:
                    if kw in node_text.lower():
                        metadata[field] = node_text
                        break
        # 递归子节点
        for child in node:
            traverse_nodes(child)

    traverse_nodes(root)
    return metadata


# -------------------------- 4. 读取Excel数据 --------------------------
def read_excel_data(excel_path, guid_column, meta_columns, sheet_name="BUS"):
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        required_columns = [guid_column] + list(meta_columns.values())
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Excel缺少必要列：{missing_cols}")

        # 处理空值
        df[guid_column] = df[guid_column].fillna("").astype(str)
        for col in meta_columns.values():
            df[col] = df[col].fillna("").astype(str)

        # 新增结果列
        df["是否存在"] = False
        df["匹配XML文件数"] = 0
        df["匹配XML文件名"] = ""

        # 构建GUID信息
        guid_info = defaultdict(lambda: {
            "metadata": {},
            "indices": [],
            "xml_files": []
        })
        for idx, row in df.iterrows():
            guid = row[guid_column]
            if guid == "":
                continue
            guid_info[guid]["metadata"] = {
                db_field: row[excel_col]
                for db_field, excel_col in meta_columns.items()
            }
            guid_info[guid]["indices"].append(idx)

        logger.info(f"Excel读取完成：{len(df)}行数据，{len(guid_info)}个非重复GUID")
        return df, guid_info

    except Exception as e:
        raise RuntimeError(f"读取Excel失败：{str(e)}")


# -------------------------- 5. 获取所有XML文件 --------------------------
def get_all_xml_files(folder_path):
    xml_files = []
    for root_dir, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".xml"):
                xml_files.append(os.path.abspath(os.path.join(root_dir, file)))
    logger.info(f"扫描到XML文件：{len(xml_files)}个")
    return xml_files


# -------------------------- 6. 解析XML并匹配GUID（存储文件名） --------------------------
def parse_xml_and_match_guids(xml_file, guid_info, db_path):
    matched_guids = set()
    # 提取完整路径和纯文件名
    xml_file_path = xml_file  # 完整路径（如D:/a/b.xml）
    xml_file_name = os.path.basename(xml_file)  # 纯文件名（如b.xml）

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except Exception as e:
        logger.warning(f"解析XML失败「{xml_file_name}」：{str(e)}")
        return matched_guids

    # 1. 提取XML元数据
    xml_metadata = extract_xml_metadata(root)

    # 2. 遍历节点匹配GUID
    matches = []

    def traverse_nodes(node, parent_path=""):
        current_path = f"{parent_path}/{node.tag}" if parent_path else node.tag
        for attr_name, attr_value in node.attrib.items():
            if attr_value in guid_info:
                matches.append({
                    "guid": attr_value,
                    "node_path": current_path,
                    "attribute": attr_name
                })
                matched_guids.add(attr_value)
                guid_info[attr_value]["xml_files"].append(xml_file_name)
        for child in node:
            traverse_nodes(child, current_path)

    traverse_nodes(root)

    # 3. 写入数据库（新增xml_file_name字段）
    if matches:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        try:
            # 写入XML元数据（含xml_file_name）
            cursor.execute('''
            INSERT OR IGNORE INTO xml_metadata 
            (xml_file_path, xml_file_name, physical_port, message_name, dp_name, full_name, parse_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                xml_file_path,
                xml_file_name,  # 新增：存储纯文件名
                xml_metadata["physical_port"],
                xml_metadata["message_name"],
                xml_metadata["dp_name"],
                xml_metadata["full_name"],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

            # 写入GUID-XML映射
            mapping_data = [
                (m["guid"], xml_file_path, m["node_path"], m["attribute"])
                for m in matches
            ]
            cursor.executemany('''
            INSERT OR IGNORE INTO guid_xml_mapping 
            (guid, xml_file_path, match_node_path, match_attribute)
            VALUES (?, ?, ?, ?)
            ''', mapping_data)

            # 写入Excel元数据
            for guid in matched_guids:
                info = guid_info[guid]
                meta = info["metadata"]
                cursor.execute('''
                INSERT OR IGNORE INTO excel_metadata 
                (guid, physical_port, message_name, dp_name, full_name, source_row)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    guid, meta["physical_port"], meta["message_name"],
                    meta["dp_name"], meta["full_name"], info["indices"][0]
                ))

            conn.commit()
            logger.info(f"XML处理完成「{xml_file_name}」：匹配{len(matched_guids)}个GUID")
        except Exception as e:
            logger.error(f"数据库写入失败「{xml_file_name}」：{str(e)}")
            conn.rollback()
        finally:
            conn.close()

    return matched_guids


# -------------------------- 7. 主逻辑 --------------------------
def main():
    global logger
    logger = setup_logging()
    logger.info("=" * 50 + " Excel与XML匹配（含路径+文件名双字段） " + "=" * 50)

    # 配置参数
    LOG_PATH = "excel_xml_check.log"
    DB_PATH = "xml_guid_mapping.db"
    EXCEL_PATH = r"D:\01_PROJECT\OHMS\[公开] 机载健康管理系统C版模型技术要求及仿真模型校验要求\ECMto631\附件2-CXF飞机机载健康管理系统C版蓝标模型接口表（MoICD）.xlsx"
    GUID_COLUMN = "Guid"
    META_COLUMNS = {
        "physical_port": "PhysicalPort",
        "message_name": "Word_Name/Message_Name",
        "dp_name": "DP_Name",
        "full_name": "Fullname"
    }
    XML_FOLDER = r"D:\01_PROJECT\OHMS\CXF ICD CXF AS2.0_CFG1.3"
    SHEET_NAME = "BUS"

    # 清空旧内容
    clear_file_contents(log_path=LOG_PATH, db_path=DB_PATH)

    # 初始化数据库（含xml_file_name字段）
    init_database(DB_PATH)

    # 读取Excel
    df, guid_info = read_excel_data(EXCEL_PATH, GUID_COLUMN, META_COLUMNS, SHEET_NAME)
    if not guid_info:
        logger.warning("无有效GUID，任务终止")
        df.to_excel("匹配结果汇总.xlsx", index=False)
        return

    # 获取XML文件
    xml_files = get_all_xml_files(XML_FOLDER)
    if not xml_files:
        logger.warning("无XML文件，任务终止")
        df.to_excel("匹配结果汇总.xlsx", index=False)
        return

    # 解析XML并匹配
    all_matched_guids = set()
    for xml_file in tqdm(xml_files, desc="解析XML并匹配", ncols=80):
        matched = parse_xml_and_match_guids(xml_file, guid_info, DB_PATH)
        all_matched_guids.update(matched)

    # 更新Excel
    logger.info(f"\n更新Excel：{len(all_matched_guids)}个GUID匹配成功")
    for guid in tqdm(all_matched_guids, desc="更新Excel进度", ncols=80):
        info = guid_info[guid]
        unique_xml_files = list(set(info["xml_files"]))
        xml_count = len(unique_xml_files)
        xml_names = ",".join(unique_xml_files)
        for idx in info["indices"]:
            df.at[idx, "是否存在"] = True
            df.at[idx, "匹配XML文件数"] = xml_count
            df.at[idx, "匹配XML文件名"] = xml_names

    # 保存结果
    output_excel = "匹配结果汇总.xlsx"
    df.to_excel(output_excel, index=False)
    logger.info(f"结果已保存：{output_excel}")

    # 验证：查询文件名字段
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT xml_file_path, xml_file_name FROM xml_metadata LIMIT 1")
    sample = cursor.fetchone()
    if sample:
        logger.info(f"\n验证：文件路径={sample[0]}，文件名={sample[1]}")
    conn.close()

    logger.info("=" * 50 + " 任务完成 " + "=" * 50)


if __name__ == "__main__":
    main()