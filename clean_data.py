#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清洗脚本 v2.0
功能：
1. 自动检测输入文件字段格式（boss_spider_api / batch_spider_improved / 51job）
2. 统一字段映射到标准格式
3. 解析 salary_text_raw 计算年薪
4. 清洗JD中的噪音文本
5. 去重（company_name_std + job_title + city）
6. 统一输出标准 CSV
"""

import csv
import re
import os
import sys

# 导入统一字段规范
from UNIFIED_FIELDS import (
    ALL_UNIFIED_FIELDS, CORE_FIELDS, BOSS_FIELDS, CLEANED_FIELDS,
    get_field_mapping, normalize_record,
    BOSS_API_TO_UNIFIED, BATCH_TO_UNIFIED,
)


def parse_salary(salary_text):
    """解析薪资文本，计算年薪"""
    if not salary_text or salary_text == '面议':
        return {'months': 12, 'min_year': None, 'max_year': None, 'avg_year': None}

    salary_months = 12
    months_match = re.search(r'(\d+)薪', salary_text)
    if months_match:
        salary_months = int(months_match.group(1))

    # 处理 "元/天" 格式
    if '元/天' in salary_text:
        daily_match = re.search(r'(\d+\.?\d*)[-~]?(\d+\.?\d*)?元\/天', salary_text)
        if daily_match:
            min_daily = float(daily_match.group(1))
            max_daily = float(daily_match.group(2)) if daily_match.group(2) else min_daily
            min_year = int(min_daily * 21.75 * 12)
            max_year = int(max_daily * 21.75 * 12)
            return {
                'months': salary_months,
                'min_year': min_year,
                'max_year': max_year,
                'avg_year': (min_year + max_year) // 2
            }

    # 处理 "K" 或 "万" 格式
    salary_text_normalized = salary_text.replace('－', '-').replace('～', '~').replace(' ', '')

    # 匹配各种格式
    salary_match = re.search(r'(\d+\.?\d*)[-~](\d+\.?\d*)[kK](?:·?\d+薪)?', salary_text_normalized)
    if not salary_match:
        salary_match = re.search(r'(\d+\.?\d*)[kK][-~](\d+\.?\d*)[kK]', salary_text_normalized)
    if not salary_match:
        salary_match = re.search(r'(\d+\.?\d*)[-~](\d+\.?\d*)万', salary_text_normalized)

    if salary_match:
        min_salary = float(salary_match.group(1))
        max_salary = float(salary_match.group(2))
        if '万' in salary_text[:salary_match.end()]:
            min_salary *= 10
            max_salary *= 10

        min_year = int(min_salary * 1000 * salary_months)
        max_year = int(max_salary * 1000 * salary_months)
        return {
            'months': salary_months,
            'min_year': min_year,
            'max_year': max_year,
            'avg_year': (min_year + max_year) // 2
        }

    return {'months': salary_months, 'min_year': None, 'max_year': None, 'avg_year': None}


def clean_jd_text(jd_text):
    """清洗JD中的噪音文本"""
    if not jd_text:
        return ''

    noise_patterns = [
        r'微信扫码.*',
        r'来自.*?直聘',
        r'BOSS直聘',
        r'boss报',
        r'boss分享',
        r'kanzhun.*',
        r'举报',
        r'分享',
        r'直聘',
        r'享举',
        r'\s+boss\s+',
        r'\s+直聘\s+',
        r'^\s+',
        r'\s+$',
    ]

    cleaned = jd_text
    for pattern in noise_patterns:
        cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def normalize_company_name(name):
    """标准化公司名称（用于去重匹配）"""
    if not name:
        return ''
    name = re.sub(
        r'(有限公司|股份有限公司|责任有限公司|集团|科技|网络|信息技术|电子|'
        r'系统|集成|发展|控股|投资|咨询|管理|服务|教育|文化|传媒|环境|能源|'
        r'电力|新能源|智能|数据、软件|平台)$',
        '', name
    )
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'（[^）]*）', '', name)
    return name.strip()


def make_dedup_key(job):
    """生成去重键"""
    company = job.get('company_name_std', '') or job.get('company_name_raw', '')
    company_std = normalize_company_name(company)
    job_title = job.get('job_title', '')
    city = job.get('city', '')
    return f"{company_std}_{job_title}_{city}"


def detect_input_format(input_file):
    """
    自动检测输入文件格式，返回字段映射

    Returns:
        tuple: (field_mapping, format_name)
    """
    try:
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            first_row = None
            for row in reader:
                first_row = row
                break

            if first_row is None:
                return BOSS_API_TO_UNIFIED, 'boss_api (empty file)'

            mapping = get_field_mapping(first_row)
            format_name = 'unknown'

            keys = set(first_row.keys())
            if '公司名称' in keys and '岗位名称' in keys:
                format_name = 'boss_api'
            elif 'company_name_std' in keys or 'company_name_raw' in keys:
                format_name = 'batch_spider'
            elif '公司名称' in keys and '职位名称' in keys:
                format_name = '51job'
            elif '统一社会信用代码' in keys or '法定代表人' in keys:
                format_name = 'qichacha'
            else:
                format_name = 'unknown'
                mapping = {k: k for k in keys}

            return mapping, format_name

    except Exception as e:
        print(f"⚠️ 格式检测失败，使用默认 boss_spider_api 格式: {e}")
        return BOSS_API_TO_UNIFIED, 'boss_api (fallback)'


def clean_data(input_file, output_file=None, verbose=True):
    """
    清洗数据主函数

    Args:
        input_file: 输入 CSV 文件路径
        output_file: 输出 CSV 文件路径（默认在输入文件名前加 clean_）
        verbose: 是否打印详细信息

    Returns:
        dict: 统计信息
    """
    if output_file is None:
        name, ext = os.path.splitext(input_file)
        output_file = f"{name}_cleaned.csv"

    if not os.path.exists(input_file):
        print(f"✗ 文件不存在: {input_file}")
        return None

    # 自动检测格式
    field_mapping, format_name = detect_input_format(input_file)
    if verbose:
        print(f"📋 检测到输入格式: {format_name}")
        print(f"   字段映射: {len(field_mapping)} 个字段")

    # 读取数据
    if verbose:
        print(f"📖 正在读取 {input_file}...")

    raw_jobs = []
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_jobs.append(row)

    if verbose:
        print(f"   原始记录: {len(raw_jobs)} 条")

    # 转换 + 清洗
    if verbose:
        print(f"🔄 正在清洗数据...")

    seen = set()
    cleaned_jobs = []
    stats = {
        'total': len(raw_jobs),
        'duplicates': 0,
        'valid_salary': 0,
        'valid_jd': 0,
        'format': format_name,
    }

    for i, raw_job in enumerate(raw_jobs):
        # 字段格式转换
        job = normalize_record(raw_job, field_mapping)

        # 标准化公司名（如果缺失）
        if not job.get('company_name_std'):
            raw_name = job.get('company_name_raw', '')
            job['company_name_std'] = normalize_company_name(raw_name)

        # 去重
        dedup_key = make_dedup_key(job)
        if dedup_key in seen:
            stats['duplicates'] += 1
            continue
        seen.add(dedup_key)

        # 解析薪资
        salary_text = job.get('salary', '')
        salary_info = parse_salary(salary_text)
        job['salary_months'] = salary_info['months']
        job['salary_min_year_rmb'] = salary_info['min_year'] if salary_info['min_year'] else ''
        job['salary_max_year_rmb'] = salary_info['max_year'] if salary_info['max_year'] else ''
        job['salary_avg_year_rmb'] = salary_info['avg_year'] if salary_info['avg_year'] else ''

        if salary_info['avg_year'] is not None:
            stats['valid_salary'] += 1

        # 清洗JD
        raw_jd = job.get('job_detail', '') or job.get('jd_text', '')
        cleaned_jd = clean_jd_text(raw_jd)
        job['jd_text_clean'] = cleaned_jd

        if cleaned_jd and len(cleaned_jd) > 50:
            stats['valid_jd'] += 1

        # 备注
        notes = []
        if salary_info['avg_year'] is None and salary_text and salary_text != '面议':
            notes.append(f"无法解析薪资: {salary_text}")
        if not cleaned_jd:
            notes.append("无JD描述")
        job['notes'] = '; '.join(notes)

        # 前5条打印调试信息
        if verbose and i < 5 and salary_text:
            print(f"\n  [{i+1}] 薪资原文: '{salary_text}'")
            print(f"      解析: {salary_info['months']}薪, "
                  f"{salary_info['min_year']}-{salary_info['max_year']}元/年")

        cleaned_jobs.append(job)

    stats['after_dedup'] = len(cleaned_jobs)
    stats['valid_salary_pct'] = (
        f"{stats['valid_salary']/stats['after_dedup']*100:.1f}%"
        if stats['after_dedup'] > 0 else "0%"
    )
    stats['valid_jd_pct'] = (
        f"{stats['valid_jd']/stats['after_dedup']*100:.1f}%"
        if stats['after_dedup'] > 0 else "0%"
    )

    # 保存
    if verbose:
        print(f"\n💾 正在保存到 {output_file}...")

    # 输出字段：统一核心字段 + 清洗字段
    output_fieldnames = list(ALL_UNIFIED_FIELDS)

    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=output_fieldnames,
                                 extrasaction='ignore')
        writer.writeheader()
        writer.writerows(cleaned_jobs)

    if verbose:
        print(f"\n{'='*50}")
        print(f"✅ 清洗完成！")
        print(f"   原始数据:     {stats['total']} 条")
        print(f"   去重后:      {stats['after_dedup']} 条")
        print(f"   重复记录:    {stats['duplicates']} 条")
        print(f"   有效薪资:   {stats['valid_salary']} 条 ({stats['valid_salary_pct']})")
        print(f"   有效JD:      {stats['valid_jd']} 条 ({stats['valid_jd_pct']})")
        print(f"   输入格式:    {stats['format']}")
        print(f"   输出文件:    {output_file}")
        print(f"{'='*50}")

    return stats


def main():
    """CLI 入口"""
    import argparse

    parser = argparse.ArgumentParser(description='数据清洗脚本 v2.0')
    parser.add_argument('input', nargs='?', default='data_boss.csv',
                        help='输入CSV文件（默认: data_boss.csv）')
    parser.add_argument('-o', '--output', help='输出CSV文件')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='静默模式，不打印详细信息')

    args = parser.parse_args()

    input_file = args.input
    if not os.path.exists(input_file):
        # 尝试几个常见文件名
        alternatives = ['data_boss.csv', 'boss_jobs_progress.csv',
                        'data_boss_api.csv']
        for alt in alternatives:
            if os.path.exists(alt):
                input_file = alt
                print(f"📋 使用默认输入文件: {input_file}")
                break
        else:
            print(f"✗ 未找到输入文件，请指定: python clean_data.py <file>")
            sys.exit(1)

    stats = clean_data(input_file, args.output, verbose=not args.quiet)
    if stats is None:
        sys.exit(1)


if __name__ == '__main__':
    main()
