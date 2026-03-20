#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
BOSS 直聘数据采集脚本 v5 (API拦截版)
- 使用API拦截获取数据，更稳定
- 绕过字体加密获取真实薪资
"""

from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Settings, Actions
import csv
import time
import json
import config

Settings.set_singleton_tab_obj(False)


def create_csv(output_file):
    """创建CSV"""
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
    """构建URL"""
    city = config.CITY_CODE
    job_type = '2' if config.JOB_TYPE == 'parttime' else '1'
    return f'https://www.zhipin.com/web/geek/job?query={keyword}&city={city}&jobType={job_type}&page={page}'


def extract_from_api_response(data, jobs_list):
    """从API响应中提取数据"""
    try:
        if isinstance(data, dict):
            # 常见的API响应结构
            if 'zpgeek/search/joblist' in str(data):
                # 职位列表数据
                reslist = data.get('zpgeek', {}).get('searchJobList', {}).get('jobList', [])
                for job in reslist:
                    jobs_list.append({
                        '公司名称': job.get('brandName', ''),
                        '岗位名称': job.get('jobName', ''),
                        '城市': job.get('cityName', ''),
                        '区域': job.get('areaName', ''),
                        '商圈': job.get('bizName', ''),
                        '薪资': job.get('salaryDesc', ''),  # 真实薪资
                        '经验': job.get('expName', ''),
                        '学历': job.get('degreeName', ''),
                        '领域': job.get('industryName', ''),
                        '性质': job.get(' financingStateName', ''),
                        '规模': job.get('scaleName', ''),
                        '技能标签': ','.join(job.get('skillTagList', [])),
                        '发布人名称': job.get('bossName', ''),
                        '发布人职称': job.get('bossTitle', ''),
                        '发布人活跃状态': job.get('activeTimeDesc', ''),
                        # 待获取
                        '岗位详情': '',
                        '福利标签': '',
                        '发布日期': '',
                        '发布人电话': '',
                        '公司类型': '',
                        '公司规模': '',
                        '公司阶段': '',
                        '公司人数': '',
                        '公司简介': '',
                        '公司地址': '',
                        '公司官网': '',
                    })
    except Exception as e:
        print(f"  解析API数据失败: {e}")


def api_listen_mode(dp, keyword):
    """API拦截模式"""
    jobs_list = []
    
    print(f"\n🌐 启动API监听模式...")
    
    # 开始监听
    try:
        # 监听职位列表API
        dp.listen.start('zpgeek/search/joblist.json')
        print("  ✓ 监听启动")
        
        # 访问搜索页面
        url = build_search_url(keyword)
        dp.get(url)
        
        # 等待验证
        print("\n⚠️ 请在浏览器中完成验证和登录...")
        print("⏳ 等待 20 秒...")
        time.sleep(20)
        
        # 获取监听数据
        for i in range(10):  # 最多10次
            try:
                packet = dp.listen.wait(5)
                if packet:
                    data = packet.response.body
                    if data:
                        print(f"  收到API响应 #{i+1}")
                        extract_from_api_response(data, jobs_list)
            except:
                break
        
        print(f"  ✓ API模式获取了 {len(jobs_list)} 条数据")
        
    except Exception as e:
        print(f"  API监听失败: {e}")
    
    # 如果API模式没数据，回退到DOM模式
    if not jobs_list:
        print("\n🔄 回退到DOM模式...")
        jobs_list = dom_mode(dp, keyword)
    
    return jobs_list


def dom_mode(dp, keyword):
    """DOM解析模式"""
    jobs_list = []
    
    print(f"\n🔍 DOM解析模式...")
    
    url = build_search_url(keyword)
    dp.get(url)
    
    time.sleep(5)
    
    # 滚动采集
    for scroll_idx in range(config.MAX_SCROLLS):
        try:
            # 尝试多种可能的选择器
            cards = None
            selectors = ['.job-card', '.job-item', '.search-job .job-item', '.job-card-wrapper']
            
            for sel in selectors:
                try:
                    cards = dp.eles(f'css:{sel}')
                    if cards:
                        break
                except:
                    continue
            
            if not cards:
                print(f"  第{scroll_idx+1}次: 无职位")
            
            # 提取数据
            for card in cards or []:
                try:
                    info = {
                        '公司名称': '', '岗位名称': '', '城市': '', '区域': '', '商圈': '',
                        '薪资': '', '经验': '', '学历': '', '领域': '', '性质': '', '规模': '',
                        '技能标签': '', '福利标签': '', '岗位详情': '', '发布日期': '',
                        '发布人名称': '', '发布人职称': '', '发布人电话': '', '发布人活跃状态': '',
                        '公司类型': '', '公司规模': '', '公司阶段': '', '公司人数': '',
                        '公司简介': '', '公司地址': '', '公司官网': '',
                    }
                    
                    # 提取文本
                    try:
                        info['岗位名称'] = card.text.split('\n')[0] if card.text else ''
                    except:
                        pass
                    
                    if info['岗位名称']:
                        jobs_list.append(info)
                        
                except:
                    continue
            
            print(f"  第{scroll_idx+1}次滚动: {len(jobs_list)}条")
            dp.scroll.to_bottom()
            time.sleep(2)
            
        except Exception as e:
            print(f"  滚动失败: {e}")
            break
    
    return jobs_list


def main():
    print("=" * 70)
    print("BOSS 直聘数据采集 v5".center(70))
    print("=" * 70)
    print(f"\n📋 配置:")
    print(f"  关键词: {config.SEARCH_QUERIES}")
    print(f"  城市: {config.CITY_CODE}")

    f, csv_writer = create_csv(config.BOSS_OUTPUT_FILE)

    print("\n🚀 启动浏览器...")
    dp = ChromiumPage()
    print("✓ 浏览器启动")

    all_jobs = []

    for keyword in config.SEARCH_QUERIES:
        print(f"\n{'='*50}")
        print(f"🔍 关键词: {keyword}")
        print(f"{'='*50}")
        
        # 优先尝试API模式，失败则回退到DOM模式
        jobs = api_listen_mode(dp, keyword)
        
        print(f"  ✓ '{keyword}': {len(jobs)} 条")
        all_jobs.extend(jobs)

    # 写入
    print(f"\n📝 写入 {len(all_jobs)} 条...")
    for job in all_jobs:
        csv_writer.writerow(job)

    f.close()
    print(f"\n✅ 完成: {config.BOSS_OUTPUT_FILE}")
    print(f"   共 {len(all_jobs)} 条")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n中断")
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
