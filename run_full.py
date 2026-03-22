#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
BOSS直聘 + 企查查 数据采集一键运行脚本

使用方法：
1. python run_full.py

流程：
1. 采集BOSS直聘数据（广州+兼职+多关键词）
2. 采集企查查公司信息
3. 整合数据并输出
"""

import os
import sys
import subprocess

# ==================== 配置参数 ====================
# 在 config.py 中修改配置

import config


def print_banner():
    """打印横幅"""
    print("\n" + "=" * 70)
    print(" BOSS直聘 + 企查查 数据采集工具".center(70))
    print("=" * 70)
    print(f"\n📋 配置信息:")
    print(f"  搜索关键词: {config.SEARCH_QUERIES}")
    print(f"  城市: 广州 ({config.CITY_CODE})")
    print(f"  职位类型: {'兼职' if config.JOB_TYPE == 'parttime' else '全职'}")
    print(f"  滚动次数: {config.MAX_SCROLLS}")
    print(f"\n📁 输出文件:")
    print(f"  BOSS数据: {config.BOSS_OUTPUT_FILE}")
    print(f"  企查查数据: {config.QICHACHA_OUTPUT_FILE}")
    print(f"  整合数据: {config.MERGED_OUTPUT_FILE}")
    print("\n" + "=" * 70)


def run_command(script_name, description):
    """运行命令"""
    print(f"\n{'='*70}")
    print(f" {description}".center(70))
    print('='*70)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False
        )
        print(f"\n✓ {description}完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description}失败: {e}")
        return False


def main():
    """主函数"""
    print_banner()

    # 阶段1: BOSS爬虫
    print("\n🚀 开始数据采集流程...")
    
    if not run_command('boss_spider.py', '阶段1: BOSS直聘数据采集'):
        print("\n⚠️ BOSS爬虫中断，是否继续？(y/n)")
        # 直接继续，因为BOSS数据可能已经部分采集
    
    # 检查BOSS数据
    if not os.path.exists(config.BOSS_OUTPUT_FILE):
        print(f"\n✗ 未生成BOSS数据文件: {config.BOSS_OUTPUT_FILE}")
        return
    
    print(f"\n✓ BOSS数据已保存")

    # 阶段2: 企查查爬虫
    print("\n" + "-"*70)
    print("是否继续采集企查查数据？(y/n)")
    print("  - 采集企查查: 获取公司联系方式和一般纳税人资质")
    print("  - 跳过: 直接整合现有BOSS数据")
    
    choice = input("请输入 (y/n): ").strip().lower()
    
    if choice == 'y':
        if not run_command('qichacha_spider.py', '阶段2: 企查查数据采集'):
            print("\n⚠️ 企查查采集中断，将只保留BOSS数据")
    
    # 阶段3: 数据整合
    if not run_command('data_merger.py', '阶段3: 数据整合'):
        print("\n✗ 数据整合失败")
        return

    # 完成
    print("\n" + "=" * 70)
    print("🎉 全部流程完成！".center(70))
    print("=" * 70)
    
    print("\n📁 生成的文件:")
    if os.path.exists(config.BOSS_OUTPUT_FILE):
        print(f"  ✓ {config.BOSS_OUTPUT_FILE}")
    if os.path.exists(config.QICHACHA_OUTPUT_FILE):
        print(f"  ✓ {config.QICHACHA_OUTPUT_FILE}")
    if os.path.exists(config.MERGED_OUTPUT_FILE):
        print(f"  ✓ {config.MERGED_OUTPUT_FILE}")
    if os.path.exists('data_general_taxpayer.csv'):
        print(f"  ✓ data_general_taxpayer.csv (一般纳税人)")
    
    print("\n" + "=" * 70)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
