#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
统一字段规范
所有爬虫输出统一字段格式

核心字段（所有爬虫必须包含）：
    company_name / company_name_raw    公司名称（原始）
    company_name_std                    公司名称（标准化）
    job_title                           岗位名称
    city                                城市
    district                            区域/行政区
    business_circle                     商圈
    salary                              薪资（原始文本）
    experience                          经验要求
    education                           学历要求
    publish_date                        发布日期
    source                              来源平台

BOSS 特有：
    hr_name                             HR/发布人名称
    hr_title                            HR职称
    hr_active_status                    HR活跃状态
    hr_phone                            HR电话

职位详情：
    skill_tags                          技能标签（逗号分隔）
    welfare_tags                        福利标签（逗号分隔）
    job_detail / jd_text                岗位详情/JD文本

公司信息：
    company_type                        公司类型
    company_size                        公司规模
    company_stage                       公司阶段
    company_employee_count              公司人数
    company_description                 公司简介
    company_address                     公司地址
    company_website                     公司官网

企查查 特有：
    credit_code                         统一社会信用代码
    legal_person                        法定代表人
    registered_capital                  注册资本
    taxpayer_type                       纳税人类型（一般纳税人/小规模）
    establish_date                      成立日期
    business_status                     经营状态
    tax_authority                       登记机关
    registered_address                  注册地址
    business_scope                      经营范围

清洗后新增字段（clean_data.py 输出）：
    salary_months                       薪资月数（如 13薪）
    salary_min_year_rmb                 年薪下限（元）
    salary_max_year_rmb                年薪上限（元）
    salary_avg_year_rmb                 年薪均值（元）
    jd_text_clean                       JD清洗后文本
    notes                               备注（如无法解析薪资等）
"""

# ==================== 统一字段列表 ====================

# 核心字段（所有爬虫必须包含）
CORE_FIELDS = [
    'company_name_raw',      # 公司名称（原始）
    'company_name_std',      # 公司名称（标准化）
    'job_title',             # 岗位名称
    'city',                  # 城市
    'district',              # 区域/行政区
    'business_circle',       # 商圈
    'salary',                # 薪资（原始文本）
    'experience',            # 经验要求
    'education',             # 学历要求
    'publish_date',          # 发布日期
    'source',                # 来源平台：boss / 51job / qichacha
]

# BOSS 特有字段
BOSS_FIELDS = [
    'hr_name',               # HR/发布人名称
    'hr_title',              # HR职称
    'hr_active_status',      # HR活跃状态
    'hr_phone',              # HR电话
]

# 职位详情字段
JOB_DETAIL_FIELDS = [
    'skill_tags',            # 技能标签（逗号分隔）
    'welfare_tags',          # 福利标签（逗号分隔）
    'job_detail',            # 岗位详情（原始）
    'jd_text_clean',         # JD清洗后文本
]

# 公司信息字段
COMPANY_FIELDS = [
    'company_type',          # 公司类型
    'company_size',          # 公司规模
    'company_stage',        # 公司阶段
    'company_employee_count', # 公司人数
    'company_description',   # 公司简介
    'company_address',       # 公司地址
    'company_website',       # 公司官网
]

# 企查查 特有字段
QICHACHA_FIELDS = [
    'credit_code',           # 统一社会信用代码
    'legal_person',          # 法定代表人
    'registered_capital',    # 注册资本
    'taxpayer_type',         # 纳税人类型
    'establish_date',        # 成立日期
    'business_status',       # 经营状态
    'tax_authority',         # 登记机关
    'registered_address',   # 注册地址
    'business_scope',        # 经营范围
    'company_phone',         # 公司联系电话
    'company_email',         # 公司邮箱
]

# 清洗后新增字段
CLEANED_FIELDS = [
    'salary_months',         # 薪资月数
    'salary_min_year_rmb',   # 年薪下限（元）
    'salary_max_year_rmb',   # 年薪上限（元）
    'salary_avg_year_rmb',   # 年薪均值（元）
    'notes',                 # 备注
]

# 所有字段（按类别组织）
ALL_UNIFIED_FIELDS = (
    CORE_FIELDS + BOSS_FIELDS + JOB_DETAIL_FIELDS +
    COMPANY_FIELDS + QICHACHA_FIELDS + CLEANED_FIELDS
)


# ==================== 字段映射表 ====================
# 旧格式字段名 -> 统一字段名
# 用于兼容旧的 CSV 文件读取

# boss_spider_api.py (中文列名)
BOSS_API_TO_UNIFIED = {
    '公司名称': 'company_name_raw',
    '岗位名称': 'job_title',
    '城市': 'city',
    '区域': 'district',
    '商圈': 'business_circle',
    '薪资': 'salary',
    '经验': 'experience',
    '学历': 'education',
    '发布日期': 'publish_date',
    '技能标签': 'skill_tags',
    '福利标签': 'welfare_tags',
    '岗位详情': 'job_detail',
    '发布人名称': 'hr_name',
    '发布人职称': 'hr_title',
    '发布人电话': 'hr_phone',
    '发布人活跃状态': 'hr_active_status',
    '公司类型': 'company_type',
    '公司规模': 'company_size',
    '公司阶段': 'company_stage',
    '公司人数': 'company_employee_count',
    '公司简介': 'company_description',
    '公司地址': 'company_address',
    '公司官网': 'company_website',
    '领域': 'industry',
    '性质': 'company_nature',
}

# batch_spider_improved.py 格式
BATCH_TO_UNIFIED = {
    'company_name_std': 'company_name_std',
    'company_name_raw': 'company_name_raw',
    'job_title': 'job_title',
    'city': 'city',
    'district': 'district',
    'business_circle': 'business_circle',
    'salary_text_raw': 'salary',
    'exp_req': 'experience',
    'edu_req': 'education',
    'jd_text': 'job_detail',
    'post_date': 'publish_date',
    'skill_tags': 'skill_tags',
    'welfare_tags': 'welfare_tags',
    'hr_name': 'hr_name',
    'hr_title': 'hr_title',
    'hr_active_status': 'hr_active_status',
}

# 51job 格式
JOB51_TO_UNIFIED = {
    '公司名称': 'company_name_raw',
    '职位名称': 'job_title',
    '工作地点': 'city',
    '工资': 'salary',
    '经验': 'experience',
    '学历': 'education',
    '发布时间': 'publish_date',
    '技能要求': 'skill_tags',
    '福利': 'welfare_tags',
    '职位描述': 'job_detail',
    '公司信息': 'company_description',
}

# 企查查 格式
QICHACHA_TO_UNIFIED = {
    '公司名称': 'company_name_raw',
    '统一社会信用代码': 'credit_code',
    '法定代表人': 'legal_person',
    '注册资本': 'registered_capital',
    '成立日期': 'establish_date',
    '经营状态': 'business_status',
    '联系电话': 'company_phone',
    '邮箱': 'company_email',
    '一般纳税人': 'taxpayer_type',
    '纳税人资质': 'taxpayer_type',
    '登记机关': 'tax_authority',
    '注册地址': 'registered_address',
    '经营范围': 'business_scope',
}


def get_field_mapping(row_sample):
    """
    根据输入行的字段自动检测格式，返回字段映射表

    Args:
        row_sample: dict，第一行数据（字段名->值）

    Returns:
        dict: {输入字段名: 统一字段名}
    """
    fields = set(row_sample.keys())

    # 检测 boss_spider_api 格式（有中文列名）
    boss_keys = {'公司名称', '岗位名称', '城市', '薪资'}
    if boss_keys.issubset(fields):
        return BOSS_API_TO_UNIFIED

    # 检测 batch_spider_improved 格式（有 company_name_std）
    if 'company_name_std' in fields or 'company_name_raw' in fields:
        return BATCH_TO_UNIFIED

    # 检测 51job 格式
    if '公司名称' in fields and '职位名称' in fields:
        return JOB51_TO_UNIFIED

    # 检测企查查格式
    if '统一社会信用代码' in fields or '法定代表人' in fields:
        return QICHACHA_TO_UNIFIED

    # 未知格式，返回空映射（字段名直接作为统一字段名）
    return {f: f for f in fields}


def normalize_record(row, field_mapping):
    """
    将一条记录从原始格式转换为统一格式

    Args:
        row: dict，原始记录
        field_mapping: dict，字段映射

    Returns:
        dict，统一格式记录
    """
    normalized = {}
    for old_key, value in row.items():
        new_key = field_mapping.get(old_key, old_key)
        normalized[new_key] = value

    # 补充默认值
    for field in ALL_UNIFIED_FIELDS:
        if field not in normalized:
            normalized[field] = ''

    return normalized
