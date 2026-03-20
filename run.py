#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
一键运行脚本
"""
import os
import sys
import subprocess

import config


def main():
    print("\n" + "=" * 60)
    print(" BOSS数据采集工具".center(60))
    print("=" * 60)
    print(f"\n📋 配置:")
    print(f"  关键词: {config.SEARCH_QUERIES}")
    print(f"  城市: {config.CITY_CODE}")
    print(f"  类型: {'兼职' if config.JOB_TYPE == 'parttime' else '全职'}")
    print("\n" + "=" * 60)
    
    # 选择运行模式
    print("\n请选择运行模式:")
    print("  1. API拦截模式 (推荐，更稳定)")
    print("  2. DOM解析模式 (备用)")
    print("  3. 退出")
    
    choice = input("\n请输入 (1/2/3): ").strip()
    
    if choice == '1':
        script = 'boss_spider_api.py'
    elif choice == '2':
        script = 'boss_spider.py'
    else:
        print("\n退出")
        return
    
    # 运行
    print(f"\n🚀 运行 {script}...")
    try:
        subprocess.run([sys.executable, script])
    except Exception as e:
        print(f"运行失败: {e}")

    print("\n✅ 完成!")


if __name__ == '__main__':
    main()
