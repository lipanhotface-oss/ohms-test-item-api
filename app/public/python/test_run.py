import time
from datetime import datetime

for i in range(1, 51):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"第 {i} 次执行，时间：{now}")
    time.sleep(2)
