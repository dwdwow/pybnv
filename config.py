# 因为binance数据集很大，所以需要一个工作目录来存储下载的文件
# 工作目录下的data.binance.vision目录下存储原始数据
# unzip.binance.vision目录下存储解压缩后的数据
# missing.binance.vision存储原始数据中缺失的数据
# tidy.binance.vision存储整理后的数据
# diy.binance.vision存储自定义的数据
# 一般情况下，只需要修改work_dir即可
# 如果work_dir为空，则存储在代码根目录
# 如果work_dir为～，则存储在用户目录
# 除此之外，work_dir为绝对路径

# 一般情况下原始zip数据直接解压，不需要存储

import os

work_dir = "~"

if work_dir == "~":
    work_dir = os.path.expanduser("~")
    
if work_dir != "" and not work_dir.startswith("/"):
    raise ValueError("work_dir must be an absolute path")

# 一般不要修改以下目录
data_binance_vision_dir = os.path.join(work_dir, "data.binance.vision")
unzip_binance_vision_dir = os.path.join(work_dir, "unzip.binance.vision")
missing_binance_vision_dir = os.path.join(work_dir, "missing.binance.vision")
tidy_binance_vision_dir = os.path.join(work_dir, "tidy.binance.vision")
diy_binance_vision_dir = os.path.join(work_dir, "diy.binance.vision")
