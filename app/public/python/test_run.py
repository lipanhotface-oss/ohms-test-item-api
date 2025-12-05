import time
from datetime import datetime
# -*- coding: utf-8 -*-
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)


for i in range(1, 51):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"第 {i} 次执行，时间：{now}")
    time.sleep(0.1)