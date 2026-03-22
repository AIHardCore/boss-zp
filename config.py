#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
配置文件
统一管理所有配置参数
"""

# ==================== BOSS直聘爬虫配置 ====================

# 搜索关键词列表（支持多个）
SEARCH_QUERIES = [
    '软件开发',
    '云服务',
    '短视频运营',
    '会展活动',
    '咨询培训',
]

# 城市配置
# 100010000 - 北京
# 101020100 - 上海
# 101280600 - 深圳
# 101210100 - 杭州
# 101280100 - 广州
# 101270100 - 成都
CITY_CODE = '101280100'  # 广州

# 职位类型筛选
JOB_TYPE = 'parttime'  # fulltime:全职 | parttime:兼职

# 滚动次数（每次滚动会加载约15条数据）
MAX_SCROLLS = 3

# 详情页获取数量（太多会很慢）
MAX_DETAIL_COUNT = 10

# 输出文件名
BOSS_OUTPUT_FILE = 'data_boss.csv'

# ==================== 企查查爬虫配置 ====================

# 企查查搜索模式
QICHACHA_OUTPUT_FILE = 'data_qichacha.csv'

# 企查查登录账号（需要自行配置cookie）
QICHACHA_COOKIE = ''

# ==================== 数据整合配置 ====================

# 整合输出文件
MERGED_OUTPUT_FILE = 'data_merged.csv'

# ==================== 其他配置 ====================

# 浏览器配置
HEADLESS = False  # 是否无头模式运行

# Chrome 程序路径（留空则让 DrissionPage 自动查找，可能找到 CentBrowser 等其他 Chromium 内核浏览器）
# Windows 示例：CHROME_BROWSER_PATH = 'C:/Program Files/Google/Chrome/Application/chrome.exe'
#              CHROME_BROWSER_PATH = 'D:/CentBrowser/Application/chrome.exe'
CHROME_BROWSER_PATH = 'C:/Program Files/Google/Chrome/Application/chrome.exe'

# Chrome 配置文件路径（留空则使用匿名模式，浏览器不保存任何数据）
# 设置后使用本地 Chrome 已登录状态，无需每次扫码登录
# 示例（Windows）：CHROME_USER_DATA_PATH = 'C:/Users/HardCore/AppData/Local/Google/Chrome/User Data/Default'
# 注意：使用配置文件时 Chrome 不能正在运行，请先关闭所有 Chrome 窗口
CHROME_USER_DATA_PATH = 'C:/Users/HardCore/AppData/Local/Google/Chrome/User Data/Default'

# API 模式最大翻页数（每页约15条）
MAX_API_PAGES = 3

# 请求间隔（秒）
REQUEST_DELAY = 2

# 是否使用大模型深度分析
USE_LLLM_ANALYSIS = False

# ==================== 增量更新配置 ====================
INCREMENTAL_MODE = False  # 临时关闭增量模式以排查问题

# ==================== 日志配置 ====================
LOG_ENABLED = True                    # 是否启用日志（True/False）
LOG_LEVEL = 'DEBUG'                  # 日志级别：DEBUG / INFO / WARNING / ERROR
LOG_TO_FILE = True                   # 是否写入文件
LOG_TO_CONSOLE = True                # 是否输出到控制台
LOG_FILE_DIR = 'logs'                # 日志目录
LOG_FILE_PREFIX = 'boss_spider'      # 日志文件前缀

# ==================== 重试配置 ====================
RETRY_MAX_RETRIES = 3                # 最大重试次数
RETRY_DELAY = 2                      # 重试间隔（秒）
RETRY_ON_NETWORK_ERROR = True        # 网络错误是否重试
RETRY_ON_CAPTCHA = True              # 遇到验证码是否重试

# ==================== 验证码配置 ====================
# 验证码检测策略：
#   manual - 检测到验证码后等待用户手动处理（检测到后打印醒目提示，按 Enter 继续）
#   auto   - 自动等待验证码消失（每5秒轮询检测一次）
#   skip   - 超时后跳过该关键词
CAPTCHA_STRATEGY = 'skip'

# 验证码最大等待时间（秒）
CAPTCHA_TIMEOUT = 60

# 验证码检测 CSS 选择器（按优先级尝试）
CAPTCHA_SELECTORS = [
    '.geetest_radar_tip',        # 滑动验证提示
    '.geetest_widget',           # 图形验证码
    '.verify-container',          # 人机验证弹窗
    '.geetest_panel',            # 极验验证码面板
    '.nc_wrapper',               # 阿里云验证
    '#nc_1_n1z',                 # 滑块缺口
    '.verification-code',         # 通用验证码
]

# ==================== AI 分析配置 ====================
AI_BATCH_SIZE = 20                 # 每批分析多少条（可配置）
AI_MODEL = 'qwen-turbo'           # 模型选择：qwen-turbo / qwen-max / gpt-4o-mini / gpt-4
AI_PROGRESS_STEP = 5               # 每多少条显示一次进度
AI_PROVIDER = 'qwen'              # AI 服务提供商：openai / qwen / ernie
AI_OUTPUT_FILE = 'data_ai_analysis.json'  # 分析结果输出文件（JSON，支持增量追加）
AI_SAVE_INTERVAL = 10             # 每分析多少条保存一次结果（防止中断丢失）
