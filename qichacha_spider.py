#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
企查查数据采集脚本
从企查查获取公司联系方式和一般纳税人资质信息

功能：
- 根据公司名称精确搜索
- 获取公司联系方式（电话、邮箱等）
- 获取一般纳税人资质信息
- 支持批量处理
- 保存为 CSV 格式
"""

import csv
import time
import json
import os
import config


def create_csv(output_file):
    """创建CSV文件"""
    f = open(file=output_file, mode='w', encoding='utf-8', newline='')
    csv_writer = csv.DictWriter(f, fieldnames=[
        '公司名称', '统一社会信用代码', '法定代表人', '注册资本',
        '成立日期', '经营状态', '联系电话', '邮箱',
        '一般纳税人', '纳税人资质', '登记机关', '注册地址',
        '经营范围', '来源'
    ])
    csv_writer.writeheader()
    return f, csv_writer


def read_company_names(input_file):
    """从BOSS数据中读取公司名称"""
    companies = set()
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('公司名称', '').strip()
                if company and company not in ['公司名称', '']:
                    # 去除公司名称中的省略号
                    company = company.replace('...', '').strip()
                    if company:
                        companies.add(company)
    except Exception as e:
        print(f"读取公司名称失败: {e}")
    
    return list(companies)


def build_search_url(company_name):
    """构建企查查搜索URL"""
    import urllib.parse
    encoded_name = urllib.parse.quote(company_name)
    return f'https://www.qcc.com/search?key={encoded_name}'


def extract_company_info(page, company_name):
    """从搜索结果提取公司信息"""
    info = {
        '公司名称': company_name,
        '统一社会信用代码': '',
        '法定代表人': '',
        '注册资本': '',
        '成立日期': '',
        '经营状态': '',
        '联系电话': '',
        '邮箱': '',
        '一般纳税人': '',
        '纳税人资质': '',
        '登记机关': '',
        '注册地址': '',
        '经营范围': '',
        '来源': '企查查'
    }
    
    try:
        # 查找搜索结果中的公司卡片
        result_cards = page.eles('css:.search-result .company-item')
        
        if not result_cards:
            # 尝试其他选择器
            result_cards = page.eles('css:.company-list .company-item')
        
        if not result_cards:
            result_cards = page.eles('css:.table-list .company-item')
        
        # 查找匹配的公司
        for card in result_cards:
            try:
                name_elem = card.ele('css:.company-name')
                if name_elem and company_name in name_elem.text:
                    # 点击进入详情页
                    name_elem.click()
                    time.sleep(2)
                    
                    # 获取详情信息
                    info = extract_detail_info(page, company_name)
                    break
            except:
                continue
        
        # 如果没有找到精确匹配的，取第一个结果
        if not info['统一社会信用代码'] and result_cards:
            try:
                first_card = result_cards[0]
                first_card.click()
                time.sleep(2)
                info = extract_detail_info(page, company_name)
            except:
                pass
    
    except Exception as e:
        print(f"  提取公司信息失败: {e}")
    
    return info


def extract_detail_info(page, search_name):
    """从详情页提取公司详细信息"""
    info = {
        '公司名称': search_name,
        '统一社会信用代码': '',
        '法定代表人': '',
        '注册资本': '',
        '成立日期': '',
        '经营状态': '',
        '联系电话': '',
        '邮箱': '',
        '一般纳税人': '',
        '纳税人资质': '',
        '登记机关': '',
        '注册地址': '',
        '经营范围': '',
        '来源': '企查查'
    }
    
    try:
        # 公司名称
        name_elem = page.ele('css:.company-name')
        if name_elem:
            info['公司名称'] = name_elem.text.strip()
        
        # 统一社会信用代码
        credit_elem = page.ele('css:.credit-code')
        if credit_elem:
            info['统一社会信用代码'] = credit_elem.text.strip()
        
        # 法定代表人
        legal_elem = page.ele('css:.legal-person')
        if legal_elem:
            info['法定代表人'] = legal_elem.text.strip()
        
        # 注册资本
        capital_elem = page.ele('css:.registered-capital')
        if capital_elem:
            info['注册资本'] = capital_elem.text.strip()
        
        # 成立日期
        date_elem = page.ele('css:.establish-date')
        if date_elem:
            info['成立日期'] = date_elem.text.strip()
        
        # 经营状态
        status_elem = page.ele('css:.business-status')
        if status_elem:
            info['经营状态'] = status_elem.text.strip()
        
        # 联系电话 - 尝试多种选择器
        phone_selectors = ['css:.phone', 'css:.contact-phone', 'css:.telephone', 'css:[class*="phone"]']
        for selector in phone_selectors:
            phone_elem = page.ele(selector)
            if phone_elem:
                info['联系电话'] = phone_elem.text.strip()
                break
        
        # 邮箱 - 尝试多种选择器
        email_selectors = ['css:.email', 'css:.contact-email', 'css:[class*="email"]']
        for selector in email_selectors:
            email_elem = page.ele(selector)
            if email_elem:
                info['邮箱'] = email_elem.text.strip()
                break
        
        # 一般纳税人资质 - 在税务信息中查找
        try:
            # 尝试找到税务信息模块
            tax_section = page.ele('css:.tax-info')
            if tax_section:
                tax_text = tax_section.text
                if '一般纳税人' in tax_text:
                    info['一般纳税人'] = '是'
                    info['纳税人资质'] = '一般纳税人'
                else:
                    info['一般纳税人'] = '否'
            else:
                # 从页面文本中搜索
                page_text = page.text
                if '一般纳税人' in page_text:
                    info['一般纳税人'] = '是'
                    info['纳税人资质'] = '一般纳税人'
                else:
                    info['一般纳税人'] = '否'
        except:
            info['一般纳税人'] = '未知'
        
        # 登记机关
        register_elem = page.ele('css:.register-authority')
        if register_elem:
            info['登记机关'] = register_elem.text.strip()
        
        # 注册地址
        address_elem = page.ele('css:.register-address')
        if address_elem:
            info['注册地址'] = address_elem.text.strip()
        
        # 经营范围
        business_elem = page.ele('css:.business-scope')
        if business_elem:
            info['经营范围'] = business_elem.text.strip()
    
    except Exception as e:
        print(f"  提取详情失败: {e}")
    
    return info


def search_company_by_api(company_name):
    """
    使用API方式搜索公司（推荐）
    需要企查查API账号，如果未配置则返回空
    """
    # 这里可以接入企查查API
    # 示例API: https://api.qcc.com/api/fuzzysearch/{company_name}
    # 需要API Key
    
    # 如果没有API，返回空让浏览器爬取
    return None


def main():
    """主函数"""
    print("=" * 70)
    print("企查查数据采集工具".center(70))
    print("=" * 70)
    
    # 检查BOSS数据文件
    boss_file = config.BOSS_OUTPUT_FILE
    if not os.path.exists(boss_file):
        print(f"\n✗ 未找到BOSS数据文件: {boss_file}")
        print("  请先运行 boss_spider.py 采集BOSS数据")
        return
    
    # 读取公司名称
    print(f"\n📖 正在从 {boss_file} 读取公司名称...")
    companies = read_company_names(boss_file)
    print(f"  ✓ 找到 {len(companies)} 个公司")
    
    if not companies:
        print("  ✗ 未找到公司数据")
        return
    
    # 创建CSV
    output_file = config.QICHACHA_OUTPUT_FILE
    f, csv_writer = create_csv(output_file)
    
    print(f"\n🕷️  开始采集企查查数据...")
    print(f"  输出文件: {output_file}")
    print(f"  公司数量: {len(companies)}")
    print("\n⚠️  注意: 首次使用需要在浏览器中登录企查查账号")
    
    # 启动浏览器
    from DrissionPage import ChromiumPage
    dp = ChromiumPage()
    
    for i, company in enumerate(companies):
        print(f"\n[{i+1}/{len(companies)}] 搜索: {company}")
        
        try:
            # 访问搜索页面
            search_url = build_search_url(company)
            dp.get(search_url)
            time.sleep(config.REQUEST_DELAY)
            
            # 提取公司信息
            info = extract_company_info(dp, company)
            
            # 写入CSV
            csv_writer.writerow(info)
            
            print(f"  ✓ {info.get('公司名称', '')}")
            print(f"    电话: {info.get('联系电话', 'N/A')}")
            print(f"    一般纳税人: {info.get('一般纳税人', 'N/A')}")
            
        except Exception as e:
            print(f"  ✗ 失败: {e}")
            continue
    
    f.close()
    print(f"\n✓ 企查查数据采集完成！")
    print(f"  输出文件: {output_file}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
