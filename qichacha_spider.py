#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
企查查数据采集脚本 v2.0
从企查查获取公司联系方式和一般纳税人资质信息

功能：
- 根据公司名称精确搜索
- 获取公司联系方式（电话、邮箱等）
- 获取一般纳税人资质信息
- 支持增量更新（跳过已采集公司）
- 保存为 CSV 格式
"""

import csv
import time
import json
import os
import re

import DrissionPage
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Settings

import config
import logger as log_module
from logger import get_logger

Settings.set_singleton_tab_obj(False)

QICHACHA_PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'qichacha_progress.json'
)

log = get_logger('qichacha')

CSV_HEADERS = [
    '公司名称', '统一社会信用代码', '法定代表人', '注册资本',
    '成立日期', '经营状态', '公司类型', '公司规模', '公司阶段',
    '公司人数', '联系电话', '邮箱', '注册地址',
    '一般纳税人', '纳税人资质', '登记机关',
    '经营范围', '来源'
]

INFO_KEY_TO_CSV = {
    'GongSiMingCheng': '公司名称',
    'TongYiSheHuiXinYongDaiMa': '统一社会信用代码',
    'FaDingDaiBiaoRen': '法定代表人',
    'ZhuCeZiBen': '注册资本',
    'ChengLiRiQi': '成立日期',
    'JingYingZhuangTai': '经营状态',
    'GongSiLeiXing': '公司类型',
    'GongSiGuiMo': '公司规模',
    'GongSiJieDuan': '公司阶段',
    'GongSiRenShu': '公司人数',
    'LianXiDianHua': '联系电话',
    'YouXiang': '邮箱',
    'ZhuCeDiZhi': '注册地址',
    'YiBanNaShuiRen': '一般纳税人',
    'NaShuiRenZiZhi': '纳税人资质',
    'DengJiJiGuan': '登记机关',
    'JingYingFanWei': '经营范围',
    'LaiYuan': '来源',
}


def _job_to_csv_row(job_dict):
    return {csv_field: job_dict.get(info_key, '') for info_key, csv_field in INFO_KEY_TO_CSV.items()}


def create_csv(output_file):
    """创建CSV文件"""
    dir_path = os.path.dirname(output_file)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    f = open(file=output_file, mode='w', encoding='utf-8-sig', newline='')
    csv_writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
    csv_writer.writeheader()
    return f, csv_writer


def read_company_names_from_boss(boss_file):
    """从BOSS数据CSV读取公司名称"""
    companies = []
    if not os.path.exists(boss_file):
        log.warning(f"BOSS数据文件不存在: {boss_file}")
        return companies
    try:
        with open(boss_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            seen = set()
            for row in reader:
                company = row.get('公司名称', '').strip()
                if company and company not in seen and company != '公司名称':
                    seen.add(company)
                    companies.append(company)
        log.info(f"从BOSS数据读取到 {len(companies)} 个公司")
    except Exception as e:
        log.error(f"读取BOSS数据失败: {e}")
    return companies


def load_progress():
    if not os.path.exists(QICHACHA_PROGRESS_FILE):
        return {'completed': [], 'total': 0}
    try:
        with open(QICHACHA_PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {'completed': [], 'total': 0}


def save_progress(state):
    try:
        with open(QICHACHA_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"保存进度失败: {e}")


def _extract_qcc_fields(dp):
    """从企查查公司详情页提取字段"""
    info = {k: '' for k in INFO_KEY_TO_CSV.keys()}
    info['LaiYuan'] = '企查查'

    try:
        page_text = dp.html

        # 公司名称
        try:
            el = dp.ele('css:.company-name', timeout=3)
            if el:
                info['GongSiMingCheng'] = el.text.strip()[:200]
        except Exception:
            pass
        if not info.get('GongSiMingCheng'):
            try:
                el = dp.ele('css:h1', timeout=2)
                if el:
                    info['GongSiMingCheng'] = el.text.strip()[:200]
            except Exception:
                pass

        # 统一社会信用代码
        try:
            match = re.search(r'统一社会信用代码[：:]\s*([A-Z0-9]{18})', page_text)
            if match:
                info['TongYiSheHuiXinYongDaiMa'] = match.group(1)
        except Exception:
            pass

        # 法定代表人
        for sel in ['css:.legal-person a', 'css:.legal-person',
                    'css:[class*="legal"]', 'css:.fr']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 50:
                        info['FaDingDaiBiaoRen'] = text
                        break
            except Exception:
                continue

        # 注册资本
        for sel in ['css:.capital', 'css:.registered-capital',
                    'css:[class*="capital"]', 'css:.reg-capital']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 100:
                        info['ZhuCeZiBen'] = text
                        break
            except Exception:
                continue

        # 成立日期
        for sel in ['css:.establish-date', 'css:.start-date',
                    'css:[class*="establish"]', 'css:.create-date']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 50:
                        info['ChengLiRiQi'] = text
                        break
            except Exception:
                continue

        # 经营状态
        for sel in ['css:.business-status', 'css:.status',
                    'css:[class*="status"]', 'css:.company-status']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 30:
                        info['JingYingZhuangTai'] = text
                        break
            except Exception:
                continue

        # 公司类型
        for sel in ['css:.company-type', 'css:.ent-type',
                    'css:[class*="company-type"]', 'css:.type']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 50:
                        info['GongSiLeiXing'] = text
                        break
            except Exception:
                continue

        # 公司规模
        for sel in ['css:.company-size', 'css:.staff',
                    'css:[class*="staff"]', 'css:.employee-count']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 50:
                        info['GongSiGuiMo'] = text
                        break
            except Exception:
                continue

        # 联系电话
        for sel in ['css:.phone-num', 'css:.contact-phone',
                    'css:.telephone', 'css:[class*="phone"]']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    # Filter to only actual phone numbers
                    phone_match = re.search(r'[\d\-\(\)]{7,20}', text)
                    if phone_match:
                        info['LianXiDianHua'] = phone_match.group()
                        break
            except Exception:
                continue

        # 邮箱
        for sel in ['css:.email', 'css:.contact-email',
                    'css:[class*="email"]']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if '@' in text:
                        info['YouXiang'] = text
                        break
            except Exception:
                continue

        # 注册地址
        for sel in ['css:.address', 'css:.register-address',
                    'css:.company-address', 'css:[class*="address"]']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 300:
                        info['ZhuCeDiZhi'] = text
                        break
            except Exception:
                continue

        # 登记机关
        for sel in ['css:.register-authority', 'css:.authority',
                    'css:[class*="register"]', 'css:.admin-organ']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text and len(text) < 100:
                        info['DengJiJiGuan'] = text
                        break
            except Exception:
                continue

        # 经营范围
        for sel in ['css:.business-scope', 'css:.scope',
                    'css:[class*="scope"]', 'css:.business-range']:
            try:
                el = dp.ele(sel, timeout=2)
                if el:
                    text = el.text.strip()
                    if text:
                        info['JingYingFanWei'] = text[:2000]
                        break
            except Exception:
                continue

        # 一般纳税人资质 - 在税务信息区域查找
        try:
            # 查找税务信息模块
            tax_sections = [
                'css:.tax-info', 'css:.tax-status', 'css:.tax-credit',
                'css:[class*="tax"]', 'css:#taxinfo', 'css:.tax-module',
            ]
            tax_text = ''
            for sel in tax_sections:
                try:
                    el = dp.ele(sel, timeout=2)
                    if el:
                        tax_text = el.text.strip()
                        break
                except Exception:
                    continue

            if not tax_text:
                tax_text = page_text

            if '一般纳税人' in tax_text:
                info['YiBanNaShuiRen'] = '是'
                # 查找纳税人资质类型
                tax_level_match = re.search(r'纳税人资质[：:]\s*([^\s,，]{2,20})', tax_text)
                if tax_level_match:
                    info['NaShuiRenZiZhi'] = tax_level_match.group(1)
                else:
                    info['NaShuiRenZiZhi'] = '一般纳税人'
            else:
                info['YiBanNaShuiRen'] = '否'
                info['NaShuiRenZiZhi'] = '小规模纳税人'
        except Exception:
            info['YiBanNaShuiRen'] = '未知'

        # 如果从页面文本中找不到一般纳税人信息，尝试从详情模块中搜索
        if info.get('YiBanNaShuiRen') == '未知':
            try:
                # 搜索页面中所有包含"纳税人"的文本
                tax_keywords = dp.eles('css=[class*="tax"]', timeout=2)
                for kw in tax_keywords:
                    try:
                        txt = kw.text.strip()
                        if '一般纳税人' in txt:
                            info['YiBanNaShuiRen'] = '是'
                            info['NaShuiRenZiZhi'] = '一般纳税人'
                            break
                    except Exception:
                        continue
                if info.get('YiBanNaShuiRen') == '未知':
                    info['YiBanNaShuiRen'] = '否'
            except Exception:
                pass

    except Exception as e:
        log.debug(f"提取企查查字段失败: {e}")

    return info


def search_company_on_qcc(dp, company_name):
    """在企查查搜索公司并提取信息"""
    info = {}
    info['GongSiMingCheng'] = company_name
    info['LaiYuan'] = '企查查'

    try:
        # 访问企查查搜索页
        search_url = f'https://www.qcc.com/search?key={company_name}'
        log.info(f"  搜索: {search_url}")
        dp.get(search_url, timeout=30)
        time.sleep(3)

        current_url = dp.url
        log.debug(f"  搜索页URL: {current_url}")

        # 尝试在搜索结果中找到目标公司
        # 企查查搜索结果是一个列表
        company_found = False

        # 方法1：从搜索结果列表中找到公司
        try:
            # 企查查的搜索结果在 .search-result 或 .company-list 中
            result_selectors = [
                'css:.search-result .company-item',
                'css:.company-list .company-item',
                'css:.table-list .company-item',
                'css:.search-list .company-item',
                'css:.result-item',
                'css:.company-list',
            ]
            for sel in result_selectors:
                try:
                    items = dp.eles(sel, timeout=3)
                    if items:
                        log.debug(f"  找到搜索结果选择器: {sel}, 数量: {len(items)}")
                        # 在结果中找到匹配的公司
                        for item in items:
                            try:
                                name_el = item.ele('css=.company-name', timeout=2)
                                if not name_el:
                                    name_el = item.ele('css=h3', timeout=2)
                                if not name_el:
                                    name_el = item.ele('css=a', timeout=2)
                                if name_el:
                                    found_name = name_el.text.strip()
                                    log.debug(f"  搜索结果公司: {found_name}")
                                    # 模糊匹配
                                    if (company_name in found_name or
                                        found_name in company_name or
                                        company_name.replace('有限公司', '') in found_name):
                                        log.info(f"  找到匹配公司: {found_name}")
                                        # 点击进入详情
                                        try:
                                            link = item.ele('css=a', timeout=2)
                                            if link:
                                                link.click()
                                                time.sleep(3)
                                                company_found = True
                                        except Exception:
                                            pass
                                        break
                            except Exception:
                                continue
                        if company_found:
                            break
                except Exception:
                    continue
                if company_found:
                    break
        except Exception as e:
            log.debug(f"  搜索结果提取失败: {e}")

        # 方法2：如果URL直接是公司详情页
        if not company_found and '/firm/' in dp.url:
            company_found = True
            log.debug(f"  直接进入详情页: {dp.url}")

        # 方法3：从页面获取第一个结果
        if not company_found:
            try:
                first_link = dp.ele('css=.company-item a, css=.result-item a, css=a[href*="/firm/"]', timeout=3)
                if first_link:
                    href = first_link.attr('href') or ''
                    if '/firm/' in href:
                        log.info(f"  点击第一个结果: {href}")
                        first_link.click()
                        time.sleep(3)
                        company_found = True
            except Exception:
                pass

        # 提取详情
        if company_found:
            log.info(f"  进入公司详情页: {dp.url}")
            info = _extract_qcc_fields(dp)

            # 导航回搜索页
            try:
                dp.back()
                time.sleep(2)
            except Exception:
                pass
        else:
            log.warning(f"  未找到公司: {company_name}")
            # 尝试直接访问公司详情URL（企查查URL格式）
            try:
                direct_url = f'https://www.qcc.com/company/{company_name}'
                dp.get(direct_url, timeout=15)
                time.sleep(3)
                if '/firm/' in dp.url or 'qcc.com' in dp.url:
                    info = _extract_qcc_fields(dp)
            except Exception:
                pass

    except Exception as e:
        log.debug(f"  搜索公司失败: {e}")

    return info


def main():
    log.info("=" * 70)
    log.info("企查查数据采集 v2.0".center(50))
    log.info("=" * 70)

    # 读取BOSS数据中的公司名称
    boss_file = config.BOSS_OUTPUT_FILE
    companies = read_company_names_from_boss(boss_file)

    if not companies:
        log.error("未找到公司数据，请先运行BOSS直聘爬虫")
        return

    # 加载进度
    progress = load_progress()
    completed = set(progress.get('completed', []))
    total = len(companies)
    log.info(f"待采集: {total} 个公司，已完成: {len(completed)} 个")

    # 过滤未完成的公司
    companies_to_process = [c for c in companies if c not in completed]
    log.info(f"本次将采集: {len(companies_to_process)} 个公司")

    if not companies_to_process:
        log.info("所有公司已采集完成")
        return

    # 创建CSV
    output_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        getattr(config, 'QICHACHA_OUTPUT_FILE', 'data_qichacha.csv')
    )

    csv_existed = os.path.exists(output_file)
    f, csv_writer = create_csv(output_file)
    if csv_existed:
        # 追加模式需要跳过已存在的公司
        try:
            with open(output_file, 'r', encoding='utf-8-sig') as cf:
                reader = csv.DictReader(cf)
                for row in reader:
                    existing_company = row.get('公司名称', '').strip()
                    if existing_company and existing_company not in completed:
                        completed.add(existing_company)
        except Exception:
            pass

    # 启动浏览器
    log.info("启动浏览器...")
    try:
        co = ChromiumOptions()
        co.set_argument('--disable-blink-features=AutomationControlled')
        co.set_argument('--disable-dev-shm-usage')
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-gpu')
        co.set_argument('--lang=zh-CN')
        co.set_argument('--window-size=1920,1080')

        chrome_user_data = getattr(config, 'CHROME_USER_DATA_PATH', '')
        if chrome_user_data:
            co.set_user_data_path(chrome_user_data.rstrip('\\/'))

        chrome_browser_path = getattr(config, 'CHROME_BROWSER_PATH', '')
        if chrome_browser_path:
            co.set_browser_path(chrome_browser_path)

        dp = ChromiumPage(addr_or_opts=co)
        log.info("浏览器启动成功")

        # 访问企查查
        dp.get("https://www.qcc.com", timeout=30)
        time.sleep(3)

    except Exception as e:
        log.error(f"浏览器启动失败: {e}")
        return

    collected = 0
    failed = 0

    try:
        for i, company in enumerate(companies_to_process):
            if company in completed:
                continue

            log.info(f"[{i+1}/{len(companies_to_process)}] 采集: {company}")

            try:
                info = search_company_on_qcc(dp, company)

                if info.get('LianXiDianHua') or info.get('YiBanNaShuiRen'):
                    csv_writer.writerow(_job_to_csv_row(info))
                    f.flush()
                    collected += 1
                    log.info(f"  成功: 电话={info.get('LianXiDianHua', 'N/A')}, "
                             f"一般纳税人={info.get('YiBanNaShuiRen', 'N/A')}")
                else:
                    # 即使没找到数据也记录
                    info['YiBanNaShuiRen'] = info.get('YiBanNaShuiRen', '未找到')
                    csv_writer.writerow(_job_to_csv_row(info))
                    f.flush()
                    failed += 1
                    log.warning(f"  未找到数据")

                # 标记完成
                completed.add(company)
                progress['completed'] = list(completed)
                save_progress(progress)

                # 间隔
                time.sleep(2)

            except Exception as e:
                log.debug(f"  处理失败: {e}")
                failed += 1
                try:
                    dp.back()
                    time.sleep(2)
                except Exception:
                    pass

    except KeyboardInterrupt:
        log.warning("用户中断")

    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            dp.quit()
        except Exception:
            pass

    log.info("=" * 50)
    log.info("企查查采集完成")
    log.info(f"输出文件: {output_file}")
    log.info(f"成功: {collected} 个, 失败: {failed} 个")
    log.info("=" * 50)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("程序被中断")
    except Exception as e:
        log.error(f"未捕获异常: {e}")
        import traceback
        traceback.print_exc()
