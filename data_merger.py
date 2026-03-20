#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
数据整合脚本
将BOSS直聘数据和企查查数据整合在一起

功能：
- 根据公司名称匹配两个数据源
- 整合公司信息和职位信息
- 输出统一的CSV文件
- 支持一般纳税人筛选
"""

import csv
import os
import config


def read_boss_data(boss_file):
    """读取BOSS数据"""
    jobs = []
    try:
        with open(boss_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('公司名称', '').strip()
                if company:
                    # 标准化公司名称用于匹配
                    company_key = company.replace('...', '').replace('有限公司', '').strip()
                    row['_company_key'] = company_key
                    jobs.append(row)
    except Exception as e:
        print(f"读取BOSS数据失败: {e}")
    
    return jobs


def read_qichacha_data(qichacha_file):
    """读取企查查数据"""
    companies = {}
    try:
        with open(qichacha_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('公司名称', '').strip()
                if company:
                    # 标准化公司名称用于匹配
                    company_key = company.replace('...', '').replace('有限公司', '').strip()
                    companies[company_key] = row
    except Exception as e:
        print(f"读取企查查数据失败: {e}")
    
    return companies


def merge_data(jobs, companies):
    """整合数据"""
    merged = []
    
    for job in jobs:
        company_key = job.get('_company_key', '')
        
        # 查找匹配的企查查数据
        qichacha_info = companies.get(company_key, {})
        
        # 合并数据
        merged_row = {
            # BOSS数据 - 基本信息
            '公司名称': job.get('公司名称', ''),
            '岗位名称': job.get('岗位名称', ''),
            '城市': job.get('城市', ''),
            '区域': job.get('区域', ''),
            '商圈': job.get('商圈', ''),
            '薪资': job.get('薪资', ''),
            '经验': job.get('经验', ''),
            '学历': job.get('学历', ''),
            '发布日期': job.get('发布日期', ''),
            
            # BOSS数据 - 职位详情
            '技能标签': job.get('技能标签', ''),
            '福利标签': job.get('福利标签', ''),
            '岗位详情': job.get('岗位详情', ''),
            
            # BOSS数据 - 发布人信息
            '发布人名称': job.get('发布人名称', ''),
            '发布人职称': job.get('发布人职称', ''),
            '发布人电话': job.get('发布人电话', ''),
            '发布人活跃状态': job.get('发布人活跃状态', ''),
            
            # BOSS数据 - 公司主页信息
            '公司类型': job.get('公司类型', ''),
            '公司规模': job.get('公司规模', ''),
            '公司阶段': job.get('公司阶段', ''),
            '公司人数': job.get('公司人数', ''),
            '公司简介': job.get('公司简介', ''),
            '公司地址': job.get('公司地址', ''),
            '公司官网': job.get('公司官网', ''),
            
            # 企查查数据
            '统一社会信用代码': qichacha_info.get('统一社会信用代码', ''),
            '法定代表人': qichacha_info.get('法定代表人', ''),
            '注册资本': qichacha_info.get('注册资本', ''),
            '成立日期': qichacha_info.get('成立日期', ''),
            '经营状态': qichacha_info.get('经营状态', ''),
            '公司联系电话': qichacha_info.get('联系电话', ''),
            '公司邮箱': qichacha_info.get('邮箱', ''),
            '一般纳税人': qichacha_info.get('一般纳税人', ''),
            '纳税人资质': qichacha_info.get('纳税人资质', ''),
            '登记机关': qichacha_info.get('登记机关', ''),
            '注册地址': qichacha_info.get('注册地址', ''),
            '经营范围': qichacha_info.get('经营范围', ''),
        }
        
        merged.append(merged_row)
    
    return merged


def filter_general_taxpayer(merged_data):
    """筛选一般纳税人"""
    return [row for row in merged_data if row.get('一般纳税人') == '是']


def save_csv(data, output_file, fieldnames):
    """保存CSV"""
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  ✓ 已保存到: {output_file}")


def print_summary(merged_data, general_taxpayer_data):
    """打印统计摘要"""
    print("\n" + "=" * 50)
    print("📊 数据整合统计".center(50))
    print("=" * 50)
    print(f"  BOSS职位总数: {len(merged_data)}")
    print(f"  匹配到企查查: {len([d for d in merged_data if d.get('统一社会信用代码')])}")
    print(f"  一般纳税人: {len(general_taxpayer_data)}")
    print("=" * 50)


def main():
    """主函数"""
    print("=" * 70)
    print("数据整合工具".center(70))
    print("=" * 70)
    
    boss_file = config.BOSS_OUTPUT_FILE
    qichacha_file = config.QICHACHA_OUTPUT_FILE
    output_file = config.MERGED_OUTPUT_FILE
    
    # 检查文件
    if not os.path.exists(boss_file):
        print(f"\n✗ 未找到BOSS数据文件: {boss_file}")
        return
    
    if not os.path.exists(qichacha_file):
        print(f"\n⚠️ 未找到企查查数据文件: {qichacha_file}")
        print("  将只保存BOSS数据")
        qichacha_file = None
    
    # 读取数据
    print(f"\n📖 正在读取数据...")
    
    print(f"  读取BOSS数据: {boss_file}")
    jobs = read_boss_data(boss_file)
    print(f"    ✓ {len(jobs)} 条职位记录")
    
    companies = {}
    if qichacha_file and os.path.exists(qichacha_file):
        print(f"  读取企查查数据: {qichacha_file}")
        companies = read_qichacha_data(qichacha_file)
        print(f"    ✓ {len(companies)} 条公司记录")
    
    # 整合数据
    print(f"\n🔄 正在整合数据...")
    merged_data = merge_data(jobs, companies)
    print(f"    ✓ 整合完成: {len(merged_data)} 条")
    
    # 筛选一般纳税人
    general_taxpayer_data = filter_general_taxpayer(merged_data)
    
    # 定义所有字段
    all_fields = [
        # 基本信息
        '公司名称', '岗位名称', '城市', '区域', '商圈',
        '薪资', '经验', '学历', '发布日期',
        # 职位详情
        '技能标签', '福利标签', '岗位详情',
        # 发布人信息
        '发布人名称', '发布人职称', '发布人电话', '发布人活跃状态',
        # 公司主页信息
        '公司类型', '公司规模', '公司阶段', '公司人数',
        '公司简介', '公司地址', '公司官网',
        # 企查查数据
        '统一社会信用代码', '法定代表人', '注册资本', '成立日期',
        '经营状态', '公司联系电话', '公司邮箱',
        '一般纳税人', '纳税人资质', '登记机关', '注册地址', '经营范围',
    ]
    
    save_csv(merged_data, output_file, all_fields)
    
    # 保存一般纳税人数据
    if general_taxpayer_data:
        tax_file = 'data_general_taxpayer.csv'
        save_csv(general_taxpayer_data, tax_file, all_fields)
    
    # 打印统计
    print_summary(merged_data, general_taxpayer_data)
    
    print(f"\n✓ 数据整合完成！")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
