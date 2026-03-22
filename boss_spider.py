#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
BOSS 直聘数据采集脚本 v4 (修复版)
- 多选择器适配
- 增加调试输出
- API拦截模式
"""

from DrissionPage import ChromiumPage
from DrissionPage.common import Settings
import csv
import time
import json
import config

Settings.set_singleton_tab_obj(False)


# 多种CSS选择器（适配不同页面版本）
SELECTORS = {
    'job_card': [
        '.job-card-wrap',
        '.job-card',
        '.job-card-wrapper',
        '.job-list .job-card',
        '.search-job .job-item',
        '.job-list-box',
        'ul.rec-job-list > li',
    ],
    'job_title': [
        '.job-title',
        '.job-name',
        '.title',
        'h3 a',
    ],
    'company_name': [
        '.company-name',
        '.company-text',
        '.company',
        '.corp-name',
    ],
    'salary': [
        '.salary',
        '.job-salary',
        '.pay',
        '[class*="salary"]',
    ],
    'area': [
        '.job-area',
        '.area',
        '.location',
    ],
    'biz_circle': [
        '.biz-circle',
        '.circle',
    ],
    'tags': [
        '.tag-item',
        '.tags',
        '.job-tags',
    ],
}


def try_selectors(page_or_ele, element_type, selectors):
    """尝试多个选择器"""
    for sel in selectors:
        try:
            if element_type == 'ele':
                result = page_or_ele.ele(f'css:{sel}')
            else:
                result = page_or_ele.eles(f'css:{sel}')
            if result:
                return result
        except:
            continue
    return None


def create_csv(output_file):
    """创建CSV文件"""
    f = open(file=output_file, mode='w', encoding='utf-8-sig', newline='')
    csv_writer = csv.DictWriter(f, fieldnames=[
        '公司名称', '岗位名称', '城市', '区域', '商圈',
        '薪资', '经验', '学历', '领域', '性质', '规模',
        '技能标签', '福利标签', '岗位详情', '发布日期',
        '发布人名称', '发布人职称', '发布人电话', '发布人活跃状态',
        '公司类型', '公司规模', '公司阶段', '公司人数',
        '公司简介', '公司地址', '公司官网',
    ])
    csv_writer.writeheader()
    return f, csv_writer


def build_search_url(keyword, page=1):
    """构建搜索URL"""
    city = config.CITY_CODE
    job_type = '1903' if config.JOB_TYPE == 'parttime' else '1902'
    return f'https://www.zhipin.com/web/geek/job?query={keyword}&city={city}&jobType={job_type}'


def extract_job_card(card):
    """从职位卡片提取信息"""
    info = {}
    
    try:
        # 岗位名称
        elem = try_selectors(card, 'ele', SELECTORS['job_title'])
        info['岗位名称'] = elem.text.strip() if elem else ''
        
        # 公司名称
        elem = try_selectors(card, 'ele', SELECTORS['company_name'])
        info['公司名称'] = elem.text.strip() if elem else ''
        
        # 薪资
        elem = try_selectors(card, 'ele', SELECTORS['salary'])
        info['薪资'] = elem.text.strip() if elem else ''
        
        # 城市/区域
        elem = try_selectors(card, 'ele', SELECTORS['area'])
        if elem:
            text = elem.text.strip()
            if '·' in text:
                parts = text.split('·')
                info['城市'] = parts[0].strip()
                info['区域'] = parts[1].strip() if len(parts) > 1 else ''
            else:
                info['城市'] = text
                info['区域'] = ''
        else:
            info['城市'] = ''
            info['区域'] = ''
        
        # 商圈
        elem = try_selectors(card, 'ele', SELECTORS['biz_circle'])
        info['商圈'] = elem.text.strip() if elem else ''
        
        # 经验/学历标签
        elems = try_selectors(card, 'eles', SELECTORS['tags'])
        if elems and len(elems) >= 2:
            info['经验'] = elems[0].text.strip()
            info['学历'] = elems[1].text.strip()
        else:
            info['经验'] = ''
            info['学历'] = ''
        
        # 待获取字段
        for k in ['领域', '性质', '规模', '技能标签', '福利标签', '岗位详情', '发布日期',
                  '发布人名称', '发布人职称', '发布人电话', '发布人活跃状态',
                  '公司类型', '公司规模', '公司阶段', '公司人数', '公司简介', '公司地址', '公司官网']:
            info[k] = ''
        
        return info if info.get('岗位名称') or info.get('公司名称') else None
        
    except Exception as e:
        print(f"    解析卡片失败: {e}")
        return None


def scroll_and_collect(page, max_scrolls):
    """滚动收集职位"""
    jobs_data = []
    
    print(f"\n🕷️ 开始滚动采集，最多 {max_scrolls} 次...")
    
    # 等待页面加载
    time.sleep(3)
    
    for scroll_idx in range(max_scrolls):
        try:
            # 尝试多种选择器
            job_cards = None
            for sel in SELECTORS['job_card']:
                try:
                    job_cards = page.eles(f'css:{sel}')
                    if job_cards:
                        print(f"  使用选择器: {sel}, 找到 {len(job_cards)} 个职位")
                        break
                except:
                    continue
            
            if not job_cards:
                print(f"  第 {scroll_idx + 1} 次: 未找到职位卡片")
            
            # 提取数据
            for card in job_cards or []:
                job_info = extract_job_card(card)
                if job_info:
                    jobs_data.append(job_info)
            
            current_count = len(jobs_data)
            print(f"  第 {scroll_idx + 1} 次滚动，当前已收集 {current_count} 个职位")
            
            # 滚动
            page.scroll.to_bottom()
            time.sleep(2)
            
        except Exception as e:
            print(f"  滚动失败: {e}")
            break
    
    return jobs_data


def main():
    """主函数"""
    print("=" * 70)
    print("BOSS 直聘数据采集工具 v4".center(70))
    print("=" * 70)
    print(f"\n📋 配置:")
    print(f"  关键词: {config.SEARCH_QUERIES}")
    print(f"  城市: {config.CITY_CODE}")
    print(f"  类型: {'兼职' if config.JOB_TYPE == 'parttime' else '全职'}")
    print(f"  滚动: {config.MAX_SCROLLS}")

    f, csv_writer = create_csv(config.BOSS_OUTPUT_FILE)

    # 启动浏览器
    print("\n🚀 启动浏览器...")
    dp = ChromiumPage()
    print("✓ 浏览器启动成功！")

    all_jobs = []

    for keyword in config.SEARCH_QUERIES:
        print(f"\n{'='*50}")
        print(f"🔍 采集关键词: {keyword}")
        print(f"{'='*50}")

        url = build_search_url(keyword)
        print(f"\n访问: {url}")
        dp.get(url)

        print("\n⚠️ 请在浏览器中:")
        print("  1. 完成人机验证")
        print("  2. 登录账号")
        print("\n⏳ 15秒后开始抓取...")
        time.sleep(15)

        # 滚动采集
        jobs = scroll_and_collect(dp, config.MAX_SCROLLS)
        print(f"  ✓ '{keyword}' 完成: {len(jobs)} 个职位")
        all_jobs.extend(jobs)

    # 写入CSV
    print(f"\n📝 写入 {len(all_jobs)} 条数据...")
    for job in all_jobs:
        csv_writer.writerow(job)

    f.close()
    print(f"\n✅ 完成！保存到: {config.BOSS_OUTPUT_FILE}")
    print(f"   总记录: {len(all_jobs)}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        traceback.print_exc()
