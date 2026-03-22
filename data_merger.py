#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据整合脚本 v2.0
功能：
1. 支持多数据源整合（BOSS / 51job / 企查查）
2. 统一字段格式
3. 增量合并（不重复合并已有数据）
4. 合并前去重（company_name + job_title + city）
5. 输出统计报告
6. 筛选一般纳税人

使用方式：
    python data_merger.py                        # 合并所有数据源
    python data_merger.py --files a.csv b.csv     # 合并指定文件
    python data_merger.py --incremental           # 增量合并（跳过已有）
    python data_merger.py --taxpayer              # 只筛选一般纳税人
"""

import csv
import os
import sys
import re
import argparse
from datetime import datetime

# 导入统一字段规范
from UNIFIED_FIELDS import (
    ALL_UNIFIED_FIELDS, CORE_FIELDS, BOSS_FIELDS,
    QICHACHA_FIELDS, COMPANY_FIELDS,
    get_field_mapping, normalize_record,
)


# ==================== 数据源配置 ====================

DEFAULT_SOURCES = {
    'boss': {
        'file': 'data_boss.csv',
        'source': 'boss',
    },
    'qichacha': {
        'file': 'data_qichacha.csv',
        'source': 'qichacha',
    },
    '51job': {
        'file': 'data_51job.csv',
        'source': '51job',
    },
}

DEFAULT_OUTPUT = 'data_merged.csv'
TAXPAYER_OUTPUT = 'data_general_taxpayer.csv'


# ==================== 工具函数 ====================

def normalize_company_name(name):
    """标准化公司名称"""
    if not name:
        return ''
    name = re.sub(
        r'(有限公司|股份有限公司|责任有限公司|集团|科技|网络|信息技术|电子|'
        r'系统|集成|发展|控股|投资|咨询|管理|服务|教育|文化|传媒|环境|能源|'
        r'电力|新能源|智能|数据|软件|平台)$',
        '', name
    )
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'（[^）]*）', '', name)
    return name.strip()


def make_dedup_key(job):
    """生成去重键"""
    company = (job.get('company_name_std', '')
               or job.get('company_name_raw', '')
               or normalize_company_name(job.get('company_name_raw', '')))
    company_std = normalize_company_name(company)
    job_title = job.get('job_title', '')
    city = job.get('city', '')
    return f"{company_std}_{job_title}_{city}"


def read_csv_records(file_path):
    """
    读取 CSV 文件，返回 (records, field_mapping)

    自动检测字段格式
    """
    if not os.path.exists(file_path):
        return [], {}

    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            return [], {}

        # 检测格式
        field_mapping = get_field_mapping(rows[0])

        # 转换记录
        normalized = []
        for row in rows:
            job = normalize_record(row, field_mapping)
            # 确保有标准化公司名
            if not job.get('company_name_std'):
                raw = job.get('company_name_raw', '')
                job['company_name_std'] = normalize_company_name(raw)
            job['_source_file'] = file_path
            normalized.append(job)

        return normalized, field_mapping

    except Exception as e:
        print(f"⚠️ 读取文件失败 {file_path}: {e}")
        return [], {}


def load_existing_merged(existing_file):
    """
    加载已有合并文件，建立去重键集合

    Returns:
        set: 已存在的去重键集合
        list: 已有记录
    """
    if not os.path.exists(existing_file):
        return set(), []

    records = []
    existing_keys = set()

    try:
        with open(existing_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)
                key = make_dedup_key(row)
                existing_keys.add(key)
    except Exception as e:
        print(f"⚠️ 读取已有合并文件失败: {e}")

    return existing_keys, records


# ==================== 数据合并 ====================

def merge_sources(sources, output_file, incremental=True,
                  filter_taxpayer=False, verbose=True):
    """
    合并多个数据源

    Args:
        sources: list of dict, 每个包含 file 和 source 字段
        output_file: str, 输出文件路径
        incremental: bool, 是否增量合并（跳过已有记录）
        filter_taxpayer: bool, 是否只保留一般纳税人
        verbose: bool, 是否打印详细信息

    Returns:
        dict: 合并统计报告
    """
    print(f"\n{'='*60}")
    print(f"📊 数据整合工具 v2.0".center(50))
    print(f"{'='*60}")

    # 收集所有数据源
    all_jobs = []
    source_stats = {}

    for src in sources:
        file_path = src['file']
        source_name = src.get('source', os.path.basename(file_path))

        if not os.path.exists(file_path):
            if verbose:
                print(f"  ⏭️  跳过（文件不存在）: {file_path}")
            continue

        records, mapping = read_csv_records(file_path)
        # 确保 source 字段有值
        for r in records:
            if not r.get('source'):
                r['source'] = source_name

        source_stats[source_name] = {
            'file': file_path,
            'total': len(records),
        }
        all_jobs.extend(records)

        if verbose:
            print(f"  ✓ {source_name}: {len(records)} 条 <- {file_path}")

    if not all_jobs:
        print(f"\n✗ 没有可合并的数据")
        return None

    # 加载已有合并文件
    existing_keys = set()
    existing_records = []
    if incremental and os.path.exists(output_file):
        existing_keys, existing_records = load_existing_merged(output_file)
        if verbose:
            print(f"\n  📦 已有合并数据: {len(existing_records)} 条")

    # 去重合并
    if verbose:
        print(f"\n🔄 正在去重合并...")

    seen_keys = set(existing_keys)
    new_records = []
    skip_stats = {}

    for job in all_jobs:
        key = make_dedup_key(job)
        source = job.get('source', 'unknown')

        if key in seen_keys:
            skip_stats[source] = skip_stats.get(source, 0) + 1
            continue

        seen_keys.add(key)
        new_records.append(job)

    # 合并：已有记录 + 新记录
    merged_records = existing_records + new_records

    # 筛选一般纳税人
    taxpayer_records = []
    if filter_taxpayer:
        taxpayer_type_field = 'taxpayer_type'
        taxpayer_records = [
            r for r in merged_records
            if r.get(taxpayer_type_field, '').strip() in ('是', '一般纳税人')
        ]

    # 输出字段
    output_fields = list(ALL_UNIFIED_FIELDS)

    # 保存合并结果
    save_csv(merged_records, output_file, output_fields)
    print(f"  ✓ 合并结果已保存: {output_file} ({len(merged_records)} 条)")

    # 保存一般纳税人
    taxpayer_output = None
    if filter_taxpayer and taxpayer_records:
        taxpayer_output = TAXPAYER_OUTPUT
        save_csv(taxpayer_records, taxpayer_output, output_fields)
        print(f"  ✓ 一般纳税人已保存: {taxpayer_output} ({len(taxpayer_records)} 条)")

    # 打印统计报告
    print_summary(merged_records, source_stats, skip_stats,
                  existing_count=len(existing_records),
                  new_count=len(new_records),
                  taxpayer_count=len(taxpayer_records))

    # 返回统计报告
    return {
        'total': len(merged_records),
        'existing': len(existing_records),
        'new': len(new_records),
        'by_source': source_stats,
        'skipped': skip_stats,
        'taxpayer': len(taxpayer_records),
        'output_file': output_file,
        'taxpayer_output': taxpayer_output,
    }


def save_csv(records, output_file, fieldnames):
    """保存 CSV"""
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)


def print_summary(merged, source_stats, skip_stats,
                  existing_count, new_count, taxpayer_count):
    """打印统计摘要"""
    print(f"\n{'='*60}")
    print(f"📊 合并统计报告".center(50))
    print(f"{'='*60}")
    print(f"  合并后总数:      {len(merged)} 条")
    print(f"  已有记录:        {existing_count} 条")
    print(f"  本次新增:        {new_count} 条")

    print(f"\n  各数据源原始数量:")
    for name, stat in source_stats.items():
        skipped = skip_stats.get(name, 0)
        print(f"    • {name}: {stat['total']} 条  (去重跳过: {skipped})")

    if taxpayer_count > 0:
        print(f"\n  一般纳税人:      {taxpayer_count} 条")

    print(f"  合并时间:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")


# ==================== CLI ====================

def main():
    parser = argparse.ArgumentParser(
        description='数据整合脚本 v2.0 - 增量合并多数据源',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python data_merger.py                              # 合并所有数据源
  python data_merger.py --files data_a.csv data_b.csv # 合并指定文件
  python data_merger.py --output merged.csv           # 指定输出文件
  python data_merger.py --no-incremental              # 重新合并（不增量）
  python data_merger.py --taxpayer                   # 只筛选一般纳税人
        '''
    )
    parser.add_argument('-f', '--files', nargs='+',
                        help='指定要合并的 CSV 文件')
    parser.add_argument('-o', '--output', default=DEFAULT_OUTPUT,
                        help=f'输出文件（默认: {DEFAULT_OUTPUT}）')
    parser.add_argument('-i', '--incremental', action='store_true',
                        default=True,
                        help='增量合并（跳过已有记录，默认开启）')
    parser.add_argument('--no-incremental', dest='incremental',
                        action='store_false',
                        help='关闭增量模式，重新合并')
    parser.add_argument('-t', '--taxpayer', action='store_true',
                        help='只保留一般纳税人')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='静默模式')

    args = parser.parse_args()

    verbose = not args.quiet

    # 确定数据源
    if args.files:
        sources = [{'file': f, 'source': os.path.basename(f)}
                   for f in args.files]
    else:
        sources = [s for s in DEFAULT_SOURCES.values()
                   if os.path.exists(s['file'])]
        if not sources:
            print(f"✗ 未找到任何数据文件")
            print(f"  请使用 --files 指定文件，或确保以下文件存在:")
            for s in DEFAULT_SOURCES.values():
                print(f"    {s['file']}")
            sys.exit(1)

    # 执行合并
    result = merge_sources(
        sources=sources,
        output_file=args.output,
        incremental=args.incremental,
        filter_taxpayer=args.taxpayer,
        verbose=verbose,
    )

    if result is None:
        sys.exit(1)

    print(f"\n✅ 合并完成！")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n⚠️ 用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
