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
MAX_SCROLLS = 20

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

# 请求间隔（秒）
REQUEST_DELAY = 2

# 是否使用大模型深度分析
USE_LLLM_ANALYSIS = False
