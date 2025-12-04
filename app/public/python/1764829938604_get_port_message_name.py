import pandas as pd

# === 1. 读取原始 Excel 文件 ===
input_file = r"D:\01_PROJECT\OHMS\[公开] 机载健康管理系统C版模型技术要求及仿真模型校验要求\ECMto631\附件2-CXF飞机机载健康管理系统C版蓝标模型接口表（MoICD）.xlsx"   # 修改为你的 Excel 文件路径
df = pd.read_excel(input_file,sheet_name="BUS")
# === 2. 去重操作 ===
# 根据 io、port、messagename 三个字段去重，只保留唯一的组合
df_unique = df.drop_duplicates(subset=["I/O", "PhysicalPort", "Word_Name/Message_Name"])
# === 3. 输出到新的 Excel 文件 ===
output_file = r"output_unique.xlsx"  # 输出路径
df_unique.to_excel(output_file, index=False)
print(f"✅ 已输出去重后的结果到: {output_file}")