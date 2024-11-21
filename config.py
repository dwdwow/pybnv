import os

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

# 一个文件的完整路径为 根目录+prefix+filename
# 例如：https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-11-19.zip
# 此url的根目录为https://data.binance.vision/
# prefix为data/spot/daily/aggTrades/BTCUSDT
# filename为BTCUSDT-aggTrades-2024-11-19.zip
# 下载时，save_path为data_binance_vision_dir+prefix+filename(zip)
# 解压时，save_path为unzip_binance_vision_dir+prefix+filename(csv)
# 缺失时，save_path为missing_binance_vision_dir+prefix+filename(csv)
# 整理时，save_path为tidy_binance_vision_dir+prefix+filename(csv)
# 这样做，可以保持文件的层次结构一样，便于管理


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


# 需要多核下载
max_workers = 1

if os.cpu_count() > 1:
    # 使用2/3的核数
    max_workers = os.cpu_count() * 2 // 3
