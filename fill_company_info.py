#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
根据data_boss.csv中的公司详情页URL，访问并回填公司工商信息到CSV
"""
import csv
import os
import re
import time
import random

import DrissionPage
from DrissionPage import ChromiumPage, ChromiumOptions

import config
from logger import get_logger

log = get_logger('fill_company')

# CSV列名映射
INFO_KEY_TO_CSV = {
    'GongSiMingCheng': '企业名称',  # 完整工商注册名称
    'TongYiSheHuiXinYongDaiMa': '统一社会信用代码',
    'FaDingDaiBiaoRen': '法定代表人',
    'ZhuCeZiBen': '注册资本',
    'ChengLiRiQi': '成立日期',
    'JingYingZhuangTai': '经营状态',
    'GongSiLeiXing': '公司类型',
    'GongSiGuiMo': '公司规模',
    'GongSiJieDuan': '公司阶段',
    'GongSiRenShu': '公司人数',
    'GongSiJianJie': '公司简介',
    'GongSiDiZhi': '公司地址',
    'GongSiGuanWang': '公司官网',
}

# data_company.csv的完整列名（含原始公司名称）
COMPANY_CSV_COLUMNS = [
    '公司名称',  # 原始卡片上的公司名称
    '企业名称',  # 工商注册完整名称
    '统一社会信用代码',
    '法定代表人',
    '注册资本',
    '成立日期',
    '经营状态',
    '公司类型',
    '公司规模',
    '公司阶段',
    '公司人数',
    '公司简介',
    '公司地址',
    '公司官网',
]


def _extract_company_fields(dp):
    """从公司详情页提取工商信息"""
    info = {k: '' for k in INFO_KEY_TO_CSV.keys()}

    # 使用innerText获取页面可见文本
    try:
        all_text = dp.run_js("return document.body.innerText")
    except:
        all_text = ''

    # ==================== 公司名称（企业名称） ====================
    if all_text:
        m = re.search(r'企业名称[：:\s]*([^\s，,。\r\n]{2,60})', all_text)
        if m:
            info['GongSiMingCheng'] = m.group(1).strip()
            log.debug(f"提取: GongSiMingCheng = {info['GongSiMingCheng']}")
    if all_text:
        m = re.search(r'公司名称[：:\s]*([^\s，,。\r\n]{2,60})', all_text)
        if m:
            info['GongSiMingCheng'] = m.group(1).strip()
            log.debug(f"提取: GongSiMingCheng = {info['GongSiMingCheng']}")

    # ==================== 正则全文提取 ====================
    if all_text:
        regex_patterns = [
            ('TongYiSheHuiXinYongDaiMa', r'统一社会信用代码[：:\s]*([A-Za-z0-9]{18})'),
            ('FaDingDaiBiaoRen', r'法定代表人[：:\s]*([^\s，,。\r\n]{2,30})'),
            ('ZhuCeZiBen', r'注册资本[：:\s]*([^\s，,。\r\n]{2,60})'),
            ('ChengLiRiQi', r'(?:成立时间|成立日期)[：:\s]*([^\s，,。\r\n]{4,30})'),
            ('JingYingZhuangTai', r'经营状态[：:\s]*([^\s，,。\r\n]{2,20})'),
            ('GongSiLeiXing', r'(?:企业类型|公司类型)[：:\s]*([^\s，,。\r\n]{2,60})'),
            ('GongSiGuiMo', r'公司规模[：:\s]*([^\s，,。\r\n]{2,30})'),
            ('GongSiJieDuan', r'(?:融资阶段|发展阶段)[：:\s]*([^\s，,。\r\n]{2,30})'),
            ('GongSiRenShu', r'员工人数[：:\s]*([^\s，,。\r\n]{2,30})'),
            ('GongSiDiZhi', r'(?:公司地址|注册地址)[：:\s]*([^\r\n]{5,300})'),
            ('GongSiGuanWang', r'公司官网[：:\s]*(https?://[^\s，,。\r\n]+)'),
        ]

        for field, pattern in regex_patterns:
            if field not in info or not info[field]:
                m = re.search(pattern, all_text)
                if m:
                    val = m.group(1).strip()
                    if val:
                        info[field] = val
                        log.debug(f"提取: {field} = {val}")

    return info


def _get_unfilled_rows(csv_file):
    """读取CSV，返回需要回填的行索引和公司URL"""
    rows = []
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            url = row.get('公司详情页URL', '').strip()
            if url:
                rows.append({
                    'row_index': i + 2,
                    'url': url,
                    'company_name': row.get('公司名称', ''),
                })
    return rows


def _get_existing_company(company_name, data_company_file='data_company.csv'):
    """根据公司名称从data_company.csv查找已存在的公司信息（精确匹配）
    返回内部键名格式的数据（用于回填）"""
    if not company_name or not os.path.exists(data_company_file):
        return None

    with open(data_company_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            saved_name = row.get('公司名称', '').strip()
            if saved_name == company_name.strip():
                return {
                    'GongSiMingCheng': row.get('企业名称', ''),
                    'TongYiSheHuiXinYongDaiMa': row.get('统一社会信用代码', ''),
                    'FaDingDaiBiaoRen': row.get('法定代表人', ''),
                    'ZhuCeZiBen': row.get('注册资本', ''),
                    'ChengLiRiQi': row.get('成立日期', ''),
                    'JingYingZhuangTai': row.get('经营状态', ''),
                    'GongSiLeiXing': row.get('公司类型', ''),
                    'GongSiGuiMo': row.get('公司规模', ''),
                    'GongSiJieDuan': row.get('公司阶段', ''),
                    'GongSiRenShu': row.get('公司人数', ''),
                    'GongSiJianJie': row.get('公司简介', ''),
                    'GongSiDiZhi': row.get('公司地址', ''),
                    'GongSiGuanWang': row.get('公司官网', ''),
                }
    return None


def _update_csv_row(csv_file, row_index, company_info):
    """更新CSV指定行的公司信息（只更新企业名称，不更新公司名称）"""
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        all_rows = list(reader)

    # 确保有企业名称列
    if '企业名称' not in headers:
        headers.append('企业名称')
        for row in all_rows:
            row['企业名称'] = ''

    # 更新对应行（更新所有工商信息字段，不更新公司名称）
    for key, csv_key in INFO_KEY_TO_CSV.items():
        if key in company_info and company_info[key]:
            all_rows[row_index - 2][csv_key] = company_info[key]

    # 写回CSV
    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_rows)


def _save_company_unique(company_info, original_company_name, output_file='data_company.csv'):
    """保存公司信息到独立CSV，自动去重（以统一社会信用代码为Key）
    - 公司名称: 原始卡片上的公司名称
    - 企业名称: 工商注册完整名称
    """
    if not company_info.get('TongYiSheHuiXinYongDaiMa'):
        return False

    key = company_info['TongYiSheHuiXinYongDaiMa']
    existing_keys = set()
    existing_rows = []

    # 读取已有数据
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_rows.append(row)
                if row.get('统一社会信用代码'):
                    existing_keys.add(row['统一社会信用代码'])

    # 去重
    if key in existing_keys:
        log.debug(f"公司已存在，跳过: {key}")
        return False

    # 追加新行
    new_row = {
        '公司名称': original_company_name,  # 原始卡片名称
        '企业名称': company_info.get('GongSiMingCheng', ''),  # 工商注册完整名称
        '统一社会信用代码': company_info.get('TongYiSheHuiXinYongDaiMa', ''),
        '法定代表人': company_info.get('FaDingDaiBiaoRen', ''),
        '注册资本': company_info.get('ZhuCeZiBen', ''),
        '成立日期': company_info.get('ChengLiRiQi', ''),
        '经营状态': company_info.get('JingYingZhuangTai', ''),
        '公司类型': company_info.get('GongSiLeiXing', ''),
        '公司规模': company_info.get('GongSiGuiMo', ''),
        '公司阶段': company_info.get('GongSiJieDuan', ''),
        '公司人数': company_info.get('GongSiRenShu', ''),
        '公司简介': company_info.get('GongSiJianJie', ''),
        '公司地址': company_info.get('GongSiDiZhi', ''),
        '公司官网': company_info.get('GongSiGuanWang', ''),
    }
    existing_rows.append(new_row)

    # 写回CSV
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COMPANY_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(existing_rows)

    log.info(f"  保存公司: {new_row['企业名称']} ({original_company_name})")
    return True


def fill_company_info(csv_file, max_rows=None):
    """访问公司详情页，回填工商信息到CSV"""
    rows_to_fill = _get_unfilled_rows(csv_file)
    if not rows_to_fill:
        log.info("没有需要回填的公司信息")
        return

    if max_rows:
        rows_to_fill = rows_to_fill[:max_rows]

    log.info(f"找到 {len(rows_to_fill)} 条需要回填的记录")

    # 启动浏览器
    co = ChromiumOptions()
    co.set_argument('--disable-blink-features=AutomationControlled')
    co.set_argument('--disable-dev-shm-usage')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--lang=zh-CN')

    if config.CHROME_BROWSER_PATH:
        co.set_browser_path(config.CHROME_BROWSER_PATH)
    if config.CHROME_USER_DATA_PATH:
        co.set_user_data_path(config.CHROME_USER_DATA_PATH)

    if config.HEADLESS:
        co.headless(True)

    dp = ChromiumPage(addr_or_opts=co)
    log.info("浏览器启动成功")

    success_count = 0
    fail_count = 0

    for i, item in enumerate(rows_to_fill):
        row_idx = item['row_index']
        url = item['url']
        company_name = item['company_name']

        # 先检查data_company.csv是否已有该公司信息
        existing = _get_existing_company(company_name)
        if existing:
            log.info(f"[{i+1}/{len(rows_to_fill)}] 已有: {company_name}，直接使用已保存信息")
            _update_csv_row(csv_file, row_idx, existing)
            success_count += 1
            continue

        log.info(f"[{i+1}/{len(rows_to_fill)}] 访问: {company_name} - {url}")

        try:
            dp.get(url, timeout=30)
            time.sleep(random.uniform(1, 3))

            company_info = _extract_company_fields(dp)
            _update_csv_row(csv_file, row_idx, company_info)
            _save_company_unique(company_info, company_name)

            if company_info.get('TongYiSheHuiXinYongDaiMa'):
                log.info(f"  成功: 信用代码={company_info['TongYiSheHuiXinYongDaiMa']}")
                success_count += 1
            else:
                log.info(f"  未提取到信用代码")
                fail_count += 1

        except Exception as e:
            log.info(f"  失败: {e}")
            fail_count += 1
            continue

        # 随机间隔，避免反爬
        if i < len(rows_to_fill) - 1:
            time.sleep(random.uniform(2, 4))

    dp.quit()
    log.info(f"完成: 成功 {success_count} 条, 失败 {fail_count} 条")


def main():
    csv_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_boss.csv')
    log.info("=" * 60)
    log.info("公司工商信息回填工具")
    log.info("=" * 60)
    fill_company_info(csv_file)


if __name__ == '__main__':
    main()
