#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hello World示例脚本
演示基本的Python脚本结构
"""

import json
import sys
import time
from datetime import datetime

def main(params):
    """主函数"""
    print("="*50)
    print("欢迎使用Python脚本平台!")
    print("="*50)
    
    # 显示当前信息
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python版本: {sys.version}")
    print(f"接收参数: {json.dumps(params, ensure_ascii=False, indent=2)}")
    
    # 演示循环输出
    print("\n开始演示循环输出:")
    for i in range(1, 6):
        print(f"进度: [{i}/5] - 正在处理...")
        time.sleep(0.5)
    
    # 演示数据处理
    print("\n演示数据处理:")
    numbers = [1, 2, 3, 4, 5]
    print(f"数据列表: {numbers}")
    print(f"总和: {sum(numbers)}")
    print(f"平均值: {sum(numbers)/len(numbers):.2f}")
    
    # 根据参数调整行为
    if params.get('debug', False):
        print("\n调试模式启用:")
        print(f"当前工作目录: {os.getcwd()}")
        print(f"环境变量: {dict(os.environ)}")
    
    print("\n" + "="*50)
    print("Hello World! 脚本执行完成!")
    print("="*50)
    
    return {
        "success": True,
        "message": "Hello World执行成功",
        "data": {
            "execution_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "processed_items": len(numbers),
            "total": sum(numbers),
            "average": sum(numbers)/len(numbers)
        }
    }

if __name__ == "__main__":
    import os
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    else:
        params = {}
    
    result = main(params)
    print(json.dumps(result, ensure_ascii=False))