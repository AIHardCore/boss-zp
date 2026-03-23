#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
BOSS直聘数据采集脚本 v6.0 (验收标准版)
- CSV中文表头
- 点击左侧岗位卡片，在右侧获取岗位详情+发布人信息
- 点击公司名称，进入公司详情页获取工商信息
- 支持增量更新，自动跳过已采集记录
"""

import csv
import time
import json
import re
import os
import signal
import sys
import random

import DrissionPage
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Settings

import config
import logger as log_module
from logger import get_logger
from utils import retry_on_failure

Settings.set_singleton_tab_obj(False)

INCREMENTAL_MODE = getattr(config, 'INCREMENTAL_MODE', True)
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'boss_progress.json')
CAPTCHA_STRATEGY = getattr(config, 'CAPTCHA_STRATEGY', 'skip')
CAPTCHA_TIMEOUT = getattr(config, 'CAPTCHA_TIMEOUT', 60)
CAPTCHA_SELECTORS = getattr(config, 'CAPTCHA_SELECTORS', [
    '.geetest_radar_tip', '.geetest_widget', '.verify-container',
    '.geetest_panel', '.nc_wrapper', '#nc_1_n1z', '.verification-code',
])

log = get_logger('boss_spider')

_global_csv_file = None
_global_csv_writer = None
_global_all_jobs = []

# CSV中文表头
CSV_HEADERS = [
    '公司名称', '统一社会信用代码', '法定代表人', '注册资本',
    '成立日期', '经营状态', '公司类型', '公司规模', '公司阶段',
    '公司人数', '公司简介', '公司地址', '公司官网',
    '岗位名称', '薪资', '地区', '区域', '商圈',
    '经验要求', '学历要求', '领域', '技能标签', '福利标签',
    '岗位详情', '发布日期',
    '发布人名称', '发布人职称', '发布人电话', '发布人活跃状态',
]

# info字段到CSV列的映射
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
    'GongSiJianJie': '公司简介',
    'GongSiDiZhi': '公司地址',
    'GongSiGuanWang': '公司官网',
    'GangWeiMingCheng': '岗位名称',
    'XinChou': '薪资',
    'ChengShi': '地区',
    'QuYu': '区域',
    'ShangQuan': '商圈',
    'JingYan': '经验要求',
    'XueLi': '学历要求',
    'LingYu': '领域',
    'JiNengBiaoQian': '技能标签',
    'FuLiBiaoQian': '福利标签',
    'GangWeiXiangQing': '岗位详情',
    'FaBuRiQi': '发布日期',
    'FaBuRenMingCheng': '发布人名称',
    'FaBuRenZhiCheng': '发布人职称',
    'FaBuRenDianHua': '发布人电话',
    'FaBuRenHuoYueZhuangTai': '发布人活跃状态',
}


def _job_to_csv_row(job_dict):
    return {csv_field: job_dict.get(info_key, '') for info_key, csv_field in INFO_KEY_TO_CSV.items()}


def _signal_handler(signum, frame):
    log.warning("Received interrupt signal, saving progress...")
    _emergency_save()
    sys.exit(0)


def _emergency_save():
    global _global_csv_file, _global_all_jobs
    if _global_csv_file:
        try:
            _global_csv_file.flush()
            os.fsync(_global_csv_file.fileno())
            log.info(f"Emergency save: {len(_global_all_jobs)} records")
        except Exception as e:
            log.error(f"Emergency save failed: {e}")


if os.name == 'nt':
    try:
        signal.signal(signal.SIGINT, _signal_handler)
    except Exception:
        pass


def normalize_company_name(name):
    if not name:
        return ''
    name = re.sub(
        r'(有限公司|股份有限公司|责任有限公司|集团|科技|网络|信息技术|电子|'
        r'System|集成|发展|控股|投资|咨询|管理|服务|教育|文化|传媒|环境|能源|'
        r'电力|新能源|智能|数据|软件|平台)$', '', name
    )
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'（[^）]*）', '', name)
    return name.strip()


def make_dedup_key(company_raw, job_title, city):
    return f"{normalize_company_name(company_raw)}_{job_title}_{city}"


@retry_on_failure(max_retries=3, delay=1, on_retry='boss_spider')
def load_progress_state():
    if not os.path.exists(PROGRESS_FILE):
        return {}
    try:
        with open(PROGRESS_FILE, 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Load progress failed: {e}")
        return {}


@retry_on_failure(max_retries=3, delay=1, on_retry='boss_spider')
def save_progress_state(state):
    try:
        dir_path = os.path.dirname(PROGRESS_FILE)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        log.error(f"Save progress failed: {e}")


def load_existing_records(csv_file):
    existing_keys = set()
    if not os.path.exists(csv_file):
        return existing_keys
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('公司名称', '').strip()
                job = row.get('岗位名称', '').strip()
                city = row.get('地区', '').strip()
                if company and job:
                    key = make_dedup_key(company, job, city)
                    existing_keys.add(key)
        log.info(f"Loaded {len(existing_keys)} historical records")
    except Exception as e:
        log.warning(f"Read historical data failed: {e}")
    return existing_keys


def create_csv(output_file, mode='w'):
    dir_path = os.path.dirname(output_file)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    f = open(file=output_file, mode=mode, encoding='utf-8-sig', newline='')
    csv_writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
    if mode == 'w':
        csv_writer.writeheader()
    return f, csv_writer


def build_search_url(keyword):
    city = config.CITY_CODE
    job_type = '1903' if config.JOB_TYPE == 'parttime' else '1902'
    return (f'https://www.zhipin.com/web/geek/job?query={keyword}'
            f'&city={city}&jobType={job_type}')


def check_captcha(dp):
    for selector in CAPTCHA_SELECTORS:
        try:
            elem = dp.ele(f'css:{selector}', timeout=2)
            if elem:
                return True, selector
        except Exception:
            continue
    return False, None


def wait_for_captcha(dp):
    if CAPTCHA_STRATEGY == 'manual':
        captcha_found, _ = check_captcha(dp)
        if not captcha_found:
            return True
        log.warning("=" * 50)
        log.warning("  [WARNING] CAPTCHA detected, please complete in browser [WARNING]")
        log.warning("=" * 50)
        try:
            input("Press Enter after completing CAPTCHA...")
            return True
        except (EOFError, KeyboardInterrupt):
            return False
    elif CAPTCHA_STRATEGY == 'auto':
        captcha_found, _ = check_captcha(dp)
        if not captcha_found:
            return True
        start_time = time.time()
        while time.time() - start_time < CAPTCHA_TIMEOUT:
            time.sleep(5)
            captcha_found, _ = check_captcha(dp)
            if not captcha_found:
                return True
        log.warning(f"CAPTCHA timeout ({CAPTCHA_TIMEOUT}s), continuing")
        return True
    else:
        return True


def _get_job_cards(dp):
    selectors = ['.job-card-wrap', '.job-card', '.job-list-box']
    for sel in selectors:
        try:
            cards = dp.eles(f'css:{sel}')
            if cards:
                return cards
        except Exception:
            continue
    return []


def _extract_list_fields(card):
    info = {k: '' for k in INFO_KEY_TO_CSV.keys()}

    # Job title
    try:
        el = card.ele('css:.job-name', timeout=2)
        if el:
            info['GangWeiMingCheng'] = el.text.strip()
    except Exception:
        pass

    # Salary - decode PUA chars from kanzhun-mix font
    try:
        el = card.ele('css:.job-salary', timeout=2)
        if el:
            salary_text = el.text.strip()
            if not salary_text or all(ord(c) > 0x1FFFF for c in salary_text):
                salary_text = (el.attr('title') or '').strip()
            # Always try to decode PUA chars (BOSS salary often has mixed encoding)
            decoded = _decode_kanzhun_salary(salary_text)
            if decoded and decoded != salary_text:
                info['XinChou'] = decoded
            else:
                info['XinChou'] = salary_text
    except Exception:
        pass

    # Company name
    try:
        el = card.ele('css:.boss-name', timeout=2)
        if el:
            info['GongSiMingCheng'] = el.text.strip()
    except Exception:
        pass

    # City.District.Business district
    try:
        el = card.ele('css:.company-location', timeout=2)
        if el:
            loc_text = el.text.strip()
            parts = [p.strip() for p in loc_text.split('\u00b7')]
            if len(parts) >= 1 and parts[0]:
                info['ChengShi'] = parts[0]
            if len(parts) >= 2 and parts[1]:
                info['QuYu'] = parts[1]
            if len(parts) >= 3 and parts[2]:
                info['ShangQuan'] = parts[2]
    except Exception:
        pass

    # Experience/Education
    try:
        tag_list = card.ele('css:.tag-list', timeout=2)
        if tag_list:
            tag_items = tag_list.eles('css:li', timeout=1)
            if len(tag_items) >= 1:
                info['JingYan'] = tag_items[0].text.strip()
            if len(tag_items) >= 2:
                info['XueLi'] = tag_items[1].text.strip()
    except Exception:
        pass

    # Skill tags
    try:
        skill_elems = card.eles('css:.tag-list > span', timeout=2)
        if skill_elems:
            info['JiNengBiaoQian'] = ','.join(s.text.strip() for s in skill_elems if s.text.strip())
    except Exception:
        pass

    # Welfare tags
    try:
        welfare_elems = card.eles('css:.welfare-tag', timeout=2)
        if not welfare_elems:
            welfare_elems = card.eles('css:.tag', timeout=2)
        if welfare_elems:
            info['FuLiBiaoQian'] = ','.join(w.text.strip() for w in welfare_elems if w.text.strip())
    except Exception:
        pass

    return info




def _decode_kanzhun_salary(text):
    """Decode BOSS直聘's kanzhun-mix font PUA chars to actual digits.
    
    BOSS uses Private Use Area characters from U+E031 to U+E03A range
    for digits 0-9. Each PUA char encodes a digit.
    
    Known mappings (verified from debug):
    U+E031 -> 0, U+E033 -> 2, U+E034 -> 3, U+E035 -> 4,
    U+E036 -> 5, U+E037 -> 6, U+E038 -> 7, U+E039 -> 8, U+E03A -> 9
    
    Also handles E032 (used for digit 1) and E03B.
    Uses formula: digit = (code - 0xE031) % 10 for E031-E03A range.
    """
    if not text:
        return ''
    result = []
    for c in text:
        cp = ord(c)
        # Handle PUA digits E031-E03A (mapped to 0-9)
        if 0xE031 <= cp <= 0xE03A:
            digit = (cp - 0xE031) % 10
            result.append(str(digit))
        # Handle E03B (encodes some special chars or digit)
        elif cp == 0xE03B:
            # E03B maps to 1 based on pattern
            result.append('1')
        elif cp <= 0xFFFF:
            result.append(c)
    decoded = ''.join(result)
    return decoded

def _extract_detail_panel(dp, card):
    """Click job card to open right panel and extract job detail + publisher info.
    
    Uses preventDefault on <a> tags to prevent full page navigation,
    allowing the SPA to update the right panel in place.
    """
    info = {}

    for attempt in range(3):
        try:
            # Click the job card - let the SPA handle the click naturally
            try:
                card.click()
            except Exception:
                pass
            time.sleep(3)

            # === Job detail: use CSS selector (more reliable than regex) ===
            # The box text includes salary (with PUA chars), location, requirements + description
            # We strip the leading non-description part (salary info + location + requirements)
            try:
                box_elem = dp.ele('css:.job-detail-box', timeout=3)
                if box_elem:
                    text = box_elem.text.strip()
                    if text:
                        # Strip the leading part: job_name salary location requirements
                        # Find "职位描述" marker to get clean job description
                        desc_marker = '职位描述'
                        if desc_marker in text:
                            idx = text.find(desc_marker)
                            clean_text = text[idx:]  # Keep from "职位描述" onwards
                        else:
                            # Fallback: strip first 100 chars (salary+location+requirements)
                            clean_text = text[100:]
                        # Remove UI elements
                        for ui_pattern in ['收藏', '举报', '微信扫码', '立即沟通']:
                            clean_text = clean_text.replace(ui_pattern, '')
                        # Remove BOSS直聘 / kanzhun / boss font artifacts from job detail
                        clean_text = re.sub(r'BOSS直聘', '', clean_text)
                        clean_text = re.sub(r'kanzhun', '', clean_text, flags=re.IGNORECASE)
                        # Remove 'boss' between ASCII letter and Chinese (boss工作周期 -> b工作周期)
                        clean_text = re.sub(r'[a-zA-Z]boss(?=[\u4e00-\u9fff])', lambda m: m.group()[0], clean_text, flags=re.IGNORECASE)
                        # Remove standalone boss/kanzhun
                        clean_text = re.sub(r'\bkanzhun\b', '', clean_text, flags=re.IGNORECASE)
                        clean_text = re.sub(r'\bboss\b', '', clean_text, flags=re.IGNORECASE)
                        # Collapse whitespace
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        info['GangWeiXiangQing'] = clean_text[:5000]
            except Exception:
                pass

            # === Salary: use CSS selector + PUA decode ===
            try:
                sal_el = dp.ele('css:.job-salary', timeout=3)
                if sal_el:
                    raw_text = (sal_el.text or '').strip()
                    if raw_text:
                        decoded = _decode_kanzhun_salary(raw_text)
                        if decoded:
                            info['XinChou'] = decoded
                    # Fallback: try title attr
                    if not info.get('XinChou'):
                        title = sal_el.attr('title')
                        if title:
                            decoded = _decode_kanzhun_salary(title.strip())
                            if decoded:
                                info['XinChou'] = decoded
            except Exception:
                pass

            # === Publisher info: use CSS selectors with fallbacks ===
            panel = dp.ele('css:.job-boss-info', timeout=3)
            if panel:
                # Active status FIRST (before extracting name, since name el includes active text)
                # <span class="boss-online-tag">在线</span>
                for sel in ['css:.boss-online-tag', 'css:.boss-active-time', 'css:.online-tag']:
                    try:
                        active_el = panel.ele(sel, timeout=2)
                        if active_el:
                            active_text = active_el.text.strip()
                            if active_text:
                                info['FaBuRenHuoYueZhuangTai'] = active_text
                                break
                    except Exception:
                        continue

                # Publisher name: <h2 class="name"> contains boss name + active tag
                # The h2 includes both text nodes, so we need to get just the name part
                # Use JS to get first text node, or strip known suffixes
                for sel in ['css:.name', 'css:h2.name', 'css:h2']:
                    try:
                        name_el = panel.ele(sel, timeout=2)
                        if name_el:
                            # Get full text (includes active status as sibling/child)
                            name_text = name_el.text.strip()
                            if name_text:
                                # The h2.name element contains "陈女士 在线" - we need just "陈女士"
                                # Try to get just the first part before any status indicator
                                # Patterns: "姓名 在线", "姓名 刚刚活跃", "姓名 已读"
                                name_text = re.sub(r'\s+(在线|刚刚活跃|已读|1分钟前|5分钟前|今日活跃|昨日活跃).*$', '', name_text)
                                name_text = re.sub(r'\s*[·•·]\s*(在线|活跃|已读).*$', '', name_text)
                                if name_text.strip():
                                    info['FaBuRenMingCheng'] = name_text.strip()
                                    break
                    except Exception:
                        continue

                # Boss title from .boss-info-attr: "公司 · 职称" format
                for sel in ['css:.boss-info-attr', 'css:.boss-title', 'css:.title']:
                    try:
                        attr_el = panel.ele(sel, timeout=2)
                        if attr_el:
                            attr_text = attr_el.text.strip()
                            if attr_text and ' · ' in attr_text:
                                parts = attr_text.split(' · ', 1)
                                # parts[0] = company name, parts[1] = boss title
                                if len(parts) > 1 and parts[1].strip():
                                    info['FaBuRenZhiCheng'] = parts[1].strip()
                                if parts[0].strip() and not info.get('GongSiMingCheng'):
                                    info['GongSiMingCheng'] = parts[0].strip()
                                break
                            elif attr_text:
                                # No separator, might be just title or company name
                                if len(attr_text) < 50 and not info.get('FaBuRenZhiCheng'):
                                    info['FaBuRenZhiCheng'] = attr_text
                                elif not info.get('GongSiMingCheng'):
                                    info['GongSiMingCheng'] = attr_text
                                break
                    except Exception:
                        continue

            info['FaBuRenDianHua'] = ''

            if info.get('GangWeiXiangQing') or info.get('FaBuRenMingCheng'):
                break

        except Exception as e:
            log.debug(f"Extract detail panel attempt {attempt+1} failed: {e}")
            time.sleep(1)

    return info



def _extract_company_fields_from_page(dp):
    """Extract company business registration info from company detail page.

    Uses regex extraction as the primary method, supplemented by CSS selectors.
    """
    info = {}

    # 1. Company name - multiple selectors
    for sel in ['css:.company-name', 'css:.info-header .name', 'css:.company-title .name',
                'css:h1[class*="company"]', 'css:.base-info .company-name', 'css:.company-tab-info h1']:
        try:
            el = dp.ele(sel, timeout=2)
            if el:
                text = el.text.strip()
                if text and len(text) < 200:
                    info['GongSiMingCheng'] = text
                    break
        except:
            continue
    if not info.get('GongSiMingCheng'):
        try:
            el = dp.ele('css:h1', timeout=2)
            if el:
                text = el.text.strip()
                if text and len(text) < 200:
                    info['GongSiMingCheng'] = text
        except:
            pass

    # 2. Regex extraction from page text (most reliable)
    # dp.text may return the page's text; try multiple text sources
    try:
        # Try dp.text first, then JS innerText as fallback
        all_text = None
        try:
            if dp.text:
                all_text = dp.text
        except:
            pass
        if not all_text:
            try:
                all_text = dp.run_js("return document.body.innerText")
            except:
                pass
        
        if all_text:
            log.debug(f"Page text length: {len(all_text)}")
            # Show a snippet around key fields
            if '统一社会信用代码' in all_text:
                idx = all_text.find('统一社会信用代码')
                log.debug(f"CreditCode context: {repr(all_text[idx:idx+80])}")
            if '法定代表人' in all_text:
                idx = all_text.find('法定代表人')
                log.debug(f"LegalRep context: {repr(all_text[idx:idx+80])}")
            
            regex_patterns = [
                ('TongYiSheHuiXinYongDaiMa', r'统一社会信用代码[：:\s]*([A-Z0-9]{18})'),
                ('FaDingDaiBiaoRen', r'法定代表人[：:\s]*([^\s，,。\r\n]{2,30})'),
                ('ZhuCeZiBen', r'注册资本[：:\s]*([^\s，,。\r\n]{2,60})'),
                ('ChengLiRiQi', r'(?:成立时间|成立日期)[：:\s]*([^\s，,。\r\n]{4,30})'),
                ('JingYingZhuangTai', r'经营状态[：:\s]*([^\s，,。\r\n]{2,20})'),
                ('GongSiLeiXing', r'企业类型[：:\s]*([^\s，,。\r\n]{2,60})'),
                ('GongSiGuiMo', r'公司规模[：:\s]*([^\s，,。\r\n]{2,30})'),
                ('GongSiDiZhi', r'(?:公司地址|注册地址)[：:\s]*([^\r\n]{5,300})'),
                ('GongSiGuanWang', r'公司官网[：:\s]*(https?://[^\s，,。\r\n]+)'),
            ]
            for field, pattern in regex_patterns:
                if field not in info:
                    m = re.search(pattern, all_text)
                    if m:
                        info[field] = m.group(1).strip()
        else:
            log.debug("Could not get page text for regex extraction")
    except Exception as e:
        log.debug(f"Regex extraction error: {e}")

    # 3. CSS selector extraction
    css_map = [
        ('FaDingDaiBiaoRen', ['css:.legal-person-name', 'css:.fr', 'css:[class*="legal"]']),
        ('ZhuCeZiBen', ['css:.registered-capital', 'css:.reg-capital', 'css:.capital', 'css:[class*="capital"]']),
        ('ChengLiRiQi', ['css:.establish-date', 'css:.start-date', 'css:.found-date']),
        ('JingYingZhuangTai', ['css:.business-status', 'css:.company-status', 'css:.status']),
        ('GongSiLeiXing', ['css:.company-type', 'css:.company-nature', 'css:.nature']),
        ('GongSiGuiMo', ['css:.company-size', 'css:.company-scale', 'css:.scale', 'css:.size']),
        ('GongSiJieDuan', ['css:.company-stage', 'css:.financing-stage', 'css:.stage']),
        ('GongSiRenShu', ['css:.employee-count', 'css:.staff-count', 'css:.count']),
        ('GongSiJianJie', ['css:.company-intro', 'css:.company-desc', 'css:.company-about', 'css:.intro-text']),
        ('GongSiDiZhi', ['css:.company-address', 'css:.company-addr', 'css:.address', 'css:.addr']),
        ('GongSiGuanWang', ['css:.company-website', 'css:.website', 'css:.company-url']),
    ]
    for field, selectors in css_map:
        if field in info and info[field]:
            continue
        for sel in selectors:
            try:
                el = dp.ele(sel, timeout=1)
                if el:
                    val = el.text.strip()
                    if val and len(val) > 1:
                        skip = ['公司规模', '融资阶段', '营业执照', '天眼查', '附近公司', '查看更多']
                        if any(p in val for p in skip):
                            continue
                        info[field] = val[:2000] if field == 'GongSiJianJie' else val[:500]
                        break
            except:
                continue

    # 4. Info-list / info-grid extraction
    try:
        items = dp.eles('css:.info-list .item, css:.info-list li, css:.base-info .row, css:.company-info .item', timeout=2)
        for item in items:
            try:
                t = item.text.strip()
                if not t or len(t) < 5:
                    continue
                for sep in ['：', ':', '——', '-']:
                    if sep in t:
                        parts = t.split(sep, 1)
                        if len(parts) == 2:
                            label, value = parts[0].strip(), parts[1].strip()
                            if not value or len(value) < 2:
                                continue
                            lm = {
                                '统一社会信用代码': 'TongYiSheHuiXinYongDaiMa',
                                '法定代表人': 'FaDingDaiBiaoRen',
                                '注册资本': 'ZhuCeZiBen',
                                '成立日期': 'ChengLiRiQi',
                                '经营状态': 'JingYingZhuangTai',
                                '公司类型': 'GongSiLeiXing',
                                '公司规模': 'GongSiGuiMo',
                                '公司人数': 'GongSiRenShu',
                                '融资阶段': 'GongSiJieDuan',
                                '公司地址': 'GongSiDiZhi',
                                '官网': 'GongSiGuanWang',
                            }
                            for lbl, fld in lm.items():
                                if lbl in label and fld not in info:
                                    info[fld] = value[:500]
                            break
            except:
                continue
    except:
        pass

    return info



def _extract_company_detail(dp, company_href, job_list_url):
    """Navigate to company detail page using pre-extracted href and extract business info.

    Uses dp.get() for navigation to ensure clean SPA state on return.
    """
    company_info = {}
    if not company_href:
        return {}

    try:
        if company_href.startswith('http'):
            full_url = company_href
        else:
            full_url = 'https://www.zhipin.com' + company_href
        if '?' in full_url:
            full_url = full_url.split('?')[0]

        log.debug(f"Navigating to company: {full_url}")
        dp.get(full_url, timeout=15)
        time.sleep(3)

        current_url = dp.url
        if '/gongsi/' in current_url and 'ka=' not in current_url:
            company_info = _extract_company_fields_from_page(dp)
            log.debug(f"Company fields: {list(company_info.keys())}")
        else:
            log.debug(f"Not a valid company page: {current_url}")

        # Use dp.back() to return to job list (preserves SPA state)
        dp.back()
        time.sleep(4)
        log.debug(f"Returned to job list: {dp.url}")
        # Click job-list-box area to ensure detail panel is active/restored
        try:
            dp.ele('css:.job-list-box', timeout=3)
            time.sleep(1)
        except Exception:
            pass

    except Exception as e:
        log.debug(f"_extract_company_detail failed: {e}")
        try:
            dp.get(job_list_url, timeout=15)
            time.sleep(3)
        except:
            pass

    return company_info






def _scroll_job_list(dp):
    dp.run_js('window.scrollBy(0, window.innerHeight)')
    time.sleep(0.5)

    for selector in ['.job-list-box', '.job-list-container', '[class*="job-list"]', '.job-card-box']:
        try:
            el = dp.ele(f'css:{selector}', timeout=2)
            if el and el.is_displayed():
                scroll_h = dp.run_js('return arguments[0].scrollHeight', el)
                client_h = dp.run_js('return arguments[0].clientHeight', el)
                if scroll_h > client_h:
                    dp.run_js('arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight', el)
                    return
        except Exception:
            continue

    dp.run_js('window.scrollBy(0, window.innerHeight)')
    time.sleep(0.5)


def _is_at_bottom(dp):
    try:
        scroll_y = dp.run_js('return window.scrollY')
        scroll_h = dp.run_js('return document.documentElement.scrollHeight')
        inner_h = dp.run_js('return window.innerHeight')
        return (scroll_y + inner_h) >= (scroll_h - 100)
    except Exception:
        return False


def collect_job_details(dp, cards, existing_keys, max_detail=5):
    all_jobs = []
    seen_keys = set(existing_keys)

    # Record job list URL before any navigation (used for returning from company pages)
    job_list_url = dp.url

    for i, card in enumerate(cards[:max_detail]):
        try:
            info = _extract_list_fields(card)
            if not info.get('GangWeiMingCheng'):
                continue

            key = make_dedup_key(
                info.get('GongSiMingCheng', ''),
                info.get('GangWeiMingCheng', ''),
                info.get('ChengShi', '')
            )

            if key in seen_keys:
                log.debug(f"Skip duplicate: {info.get('GangWeiMingCheng')} - {info.get('GangWeiMingCheng')}")
                continue

            log.info(f"  [{i+1}/{len(cards)}] Processing: {info.get('GongSiMingCheng')} - {info.get('GangWeiMingCheng')}")

            # Step 1: Extract company href from card BEFORE clicking for job detail
            company_href = None
            try:
                card_links = card.eles('css:a[href*="/gongsi/"]', timeout=2)
                for cl in card_links:
                    href = cl.attr('href') or ''
                    if '/gongsi/' in href and 'ka=' not in href and 'from=' not in href:
                        company_href = href
                        break
                if not company_href:
                    for cl in card_links:
                        href = cl.attr('href') or ''
                        if '/gongsi/' in href and 'ka=' not in href:
                            company_href = href
                            break
                if company_href:
                    log.debug(f"Company href: {company_href}")
            except Exception as e:
                log.debug(f"Error extracting company href: {e}")

            # Extract detail from right panel (job description, publisher info)
            detail_info = _extract_detail_panel(dp, card)
            info.update(detail_info)

            # Try to get company info from the right panel (no navigation yet)
            try:
                company_el = dp.ele('css:.job-company .company-name', timeout=2)
                if not company_el:
                    company_el = dp.ele('css:.boss-info-company', timeout=2)
                if company_el:
                    text = company_el.text.strip()
                    if text and not info.get('GongSiMingCheng'):
                        info['GongSiMingCheng'] = text
            except Exception:
                pass

            # Step 3: Navigate to company detail page to extract business registration info
            if company_href:
                try:
                    company_detail_info = _extract_company_detail(dp, company_href, job_list_url)
                    if company_detail_info:
                        # Only fill in missing fields
                        for k, v in company_detail_info.items():
                            if v and not info.get(k):
                                info[k] = v
                        log.debug(f"Company fields: {list(company_detail_info.keys())}")
                except Exception as e:
                    log.debug(f"Company detail failed: {e}")
            else:
                log.debug("No company href available")

            seen_keys.add(key)
            all_jobs.append(info)

            job_desc = info.get('GangWeiXiangQing', '')[:80].replace('\n', ' ')
            log.info(f"    Detail: {job_desc}...")
            log.info(f"    Publisher: {info.get('FaBuRenMingCheng')} {info.get('FaBuRenZhiCheng')} Phone: {info.get('FaBuRenDianHua')}")

            # Re-get cards after company page navigation to avoid stale references
            if i + 1 < len(cards[:max_detail]):
                try:
                    time.sleep(3)
                    fresh_cards = _get_job_cards(dp)
                    if fresh_cards and len(fresh_cards) > i + 1:
                        cards = fresh_cards
                        log.debug(f"Refreshed: {len(fresh_cards)} cards, next: card {i+2}")
                        # Scroll the target card into view and re-click to force right panel update
                        try:
                            target_card = cards[i + 1]
                            target_card.scroll.to_see()
                            time.sleep(1)
                            # First remove any persistent <a> interceptors from previous clicks
                            target_card.run_js('''
                                var links = this.querySelectorAll('a');
                                links.forEach(function(link) {
                                    // Clone and replace to remove all event listeners
                                    var clone = link.cloneNode(true);
                                    link.parentNode.replaceChild(clone, link);
                                });
                            ''')
                            time.sleep(0.5)
                            # Now click - <a> tags will work normally
                            target_card.click()
                            time.sleep(3)
                            log.debug(f"Clicked card {i+2} to update right panel")
                        except Exception as e2:
                            log.debug(f"Re-click card {i+2} failed: {e2}")
                except Exception as e:
                    log.debug(f"Card refresh failed: {e}")

        except Exception as e:
            log.debug(f"Process card {i} failed: {e}")
            continue

    return all_jobs


def scroll_and_collect(dp, keyword, max_scrolls=3, max_detail=5):
    jobs_list = []

    log.info(f"Start collecting: {keyword}")
    url = build_search_url(keyword)
    dp.get(url, timeout=30)
    time.sleep(3)

    captcha_passed = wait_for_captcha(dp)
    if not captcha_passed:
        log.warning("CAPTCHA not passed, may get partial data")

    prev_count = 0
    no_new_count = 0

    for scroll_idx in range(1, max_scrolls + 1):
        _scroll_job_list(dp)
        time.sleep(random.uniform(2, 4))

        cards = _get_job_cards(dp)
        current_count = len(cards)
        new_count = current_count - prev_count

        log.info(f"Scroll {scroll_idx}: total {current_count} (+{new_count} new)")

        if new_count <= 0:
            no_new_count += 1
            if no_new_count >= 2:
                log.info(f"No new data for {no_new_count} scrolls, stopping")
                break
        else:
            no_new_count = 0
            prev_count = current_count

        if current_count > 0 and _is_at_bottom(dp):
            log.info("Reached bottom")
            break

        if scroll_idx >= max_scrolls:
            break

    if cards:
        detail_jobs = collect_job_details(dp, cards, set(), max_detail=max_detail)
        jobs_list.extend(detail_jobs)
        log.info(f"Detail collection done: {len(detail_jobs)} records")

    if not jobs_list:
        log.warning("No data, fallback to list extraction")
        cards = _get_job_cards(dp)
        for card in cards[:max_detail]:
            info = _extract_list_fields(card)
            if info.get('GangWeiMingCheng'):
                jobs_list.append(info)

    return jobs_list


def main():
    global _global_csv_file, _global_csv_writer, _global_all_jobs

    log.info("=" * 70)
    log.info("BOSS Zhipin Data Collection v6.0 - Acceptance Test Version".center(50))
    log.info("=" * 70)
    log.info(f"Keywords: {config.SEARCH_QUERIES}")
    log.info(f"City: {config.CITY_CODE}")
    log.info(f"Job Type: {'Part-time' if config.JOB_TYPE == 'parttime' else 'Full-time'}")

    existing_keys = set()
    total_previous = 0
    if INCREMENTAL_MODE:
        existing_keys = load_existing_records(config.BOSS_OUTPUT_FILE)
        total_previous = len(existing_keys)
        if total_previous > 0:
            log.info(f"Found {total_previous} historical records")

    csv_mode = ('a' if INCREMENTAL_MODE and os.path.exists(config.BOSS_OUTPUT_FILE) else 'w')
    _global_csv_file, _global_csv_writer = create_csv(config.BOSS_OUTPUT_FILE, mode=csv_mode)

    log.info("Starting browser...")
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
            user_data = chrome_user_data.rstrip('\\/')
            co.set_user_data_path(user_data)
            log.info(f"Using Chrome profile: {chrome_user_data}")

        chrome_browser_path = getattr(config, 'CHROME_BROWSER_PATH', '')
        if chrome_browser_path:
            co.set_browser_path(chrome_browser_path)

        dp = ChromiumPage(addr_or_opts=co)
        log.info("Browser started")

        dp.get("https://www.zhipin.com/web/geek/job", timeout=30)
        time.sleep(3)
        log.info(f"Current URL: {dp.url}")

    except Exception as e:
        log.error(f"Browser start failed: {e}")
        return

    all_jobs = []
    new_count = 0
    skip_count = 0

    try:
        for keyword in config.SEARCH_QUERIES:
            log.info(f"{'=' * 50}")
            log.info(f"Collecting keyword: {keyword}")
            log.info(f"{'=' * 50}")

            jobs = scroll_and_collect(dp, keyword, max_scrolls=2, max_detail=config.MAX_DETAIL_COUNT)

            for job in jobs:
                key = make_dedup_key(
                    job.get('GongSiMingCheng', ''),
                    job.get('GangWeiMingCheng', ''),
                    job.get('ChengShi', '')
                )
                if INCREMENTAL_MODE and key in existing_keys:
                    skip_count += 1
                    continue
                existing_keys.add(key)
                all_jobs.append(job)
                new_count += 1

            log.info(f"Keyword '{keyword}': got {len(jobs)} records, {new_count} new")

            if all_jobs:
                for job_item in all_jobs:
                    _global_csv_writer.writerow(_job_to_csv_row(job_item))
                _global_csv_file.flush()
                os.fsync(_global_csv_file.fileno())
                log.info(f"Wrote {len(all_jobs)} records to CSV")
                all_jobs = []

            if INCREMENTAL_MODE and len(jobs) > 0:
                state = load_progress_state()
                if 'completed_keywords' not in state:
                    state['completed_keywords'] = []
                if keyword not in state['completed_keywords']:
                    state['completed_keywords'].append(keyword)
                state['last_keyword'] = keyword
                state['total_collected'] = new_count
                save_progress_state(state)

    except KeyboardInterrupt:
        log.warning("Ctrl+C - saving progress")

    finally:
        try:
            _global_csv_file.close()
            log.info("CSV file closed")
        except Exception:
            pass
        try:
            dp.quit()
            log.info("Browser closed")
        except Exception:
            pass

    log.info("=" * 50)
    log.info("Collection complete")
    log.info(f"Output: {config.BOSS_OUTPUT_FILE}")
    log.info(f"New records: {new_count}")
    if INCREMENTAL_MODE:
        log.info(f"Historical: {total_previous}, Skipped: {skip_count}")
    log.info("=" * 50)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Program interrupted")
    except Exception as e:
        log.error(f"Unhandled exception: {e}")
        import traceback
        traceback.print_exc()
