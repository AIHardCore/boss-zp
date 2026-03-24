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


def _safe_flush_csv():
    """安全刷新CSV文件，处理文件被占用的情况（如被Excel打开）"""
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            _global_csv_file.flush()
            os.fsync(_global_csv_file.fileno())
            return True
        except PermissionError as e:
            if attempt < max_retries:
                log.warning(f"CSV文件被占用（第{attempt}次重试），"
                            f"请确认文件未被Excel或其他程序打开，{attempt*2}秒后重试...")
                time.sleep(attempt * 2)
            else:
                log.error(f"CSV文件写入失败（文件被占用）: {e}")
                raise
    return False

# CSV中文表头
CSV_HEADERS = [
    '公司名称', '企业名称', '统一社会信用代码', '法定代表人', '注册资本',
    '成立日期', '经营状态', '公司类型', '公司规模', '公司阶段',
    '公司人数', '公司简介', '公司地址', '公司官网',
    '岗位名称', '薪资', '地区', '区域', '商圈',
    '经验要求', '学历要求', '领域', '技能标签', '福利标签',
    '岗位详情', '发布日期',
    '发布人名称', '发布人职称', '发布人电话', '发布人活跃状态',
    '公司详情页URL',
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
    '_company_href': '公司详情页URL',
}


def _job_to_csv_row(job_dict):
    return {csv_field: job_dict.get(info_key, '') for info_key, csv_field in INFO_KEY_TO_CSV.items()}


def _signal_handler(signum, frame):
    log.warning("Received interrupt signal, saving progress...")
    _emergency_save()
    sys.exit(0)


def _emergency_save():
    """紧急保存（Ctrl+C时调用）"""
    global _global_csv_file, _global_all_jobs
    if _global_csv_file:
        try:
            _safe_flush_csv()
            log.info(f"紧急保存完成: {len(_global_all_jobs)} 条记录")
        except Exception as e:
            log.error(f"紧急保存失败: {e}")


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
    """创建CSV文件，支持文件被占用时自动重试。始终保留表头行。"""
    dir_path = os.path.dirname(output_file)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    # 如果文件已存在，检查是否有表头；没有则重建文件
    if mode == 'a' and os.path.exists(output_file):
        _retry_file_operation(lambda: _ensure_bom(output_file))
        # 读取现有内容，检查第一行是否为有效的CSV头（包含"公司名称"）
        try:
            with open(output_file, 'r', encoding='utf-8-sig') as check_f:
                first_line = check_f.readline().strip()
                # 如果第一行不是表头（不含公司名称），则重建文件
                if '公司名称' not in first_line:
                    log.warning(f"CSV缺少表头行，将重建文件（现有{sum(1 for _ in open(output_file,encoding='utf-8-sig'))-1}条数据将被保留）")
                    # 读取所有现有数据
                    existing_rows = []
                    reader = csv.DictReader(open(output_file, 'r', encoding='utf-8-sig'))
                    for row in reader:
                        existing_rows.append(row)
                    # 重建文件（write模式，会覆盖）
                    mode = 'w'
                    f = _retry_file_operation(lambda: open(file=output_file, mode=mode, encoding='utf-8-sig', newline=''))
                    csv_writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                    csv_writer.writeheader()
                    # 重新写入已有数据
                    for row in existing_rows:
                        csv_writer.writerow({k: row.get(k, '') for k in CSV_HEADERS})
                    f.flush()
                    os.fsync(f.fileno())
                    return f, csv_writer
        except Exception as e:
            log.debug(f"检查CSV表头异常: {e}，按追加模式继续")

    # 打开文件（可能被Excel等占用，支持重试）
    f = _retry_file_operation(lambda: open(file=output_file, mode=mode, encoding='utf-8-sig', newline=''))
    csv_writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
    if mode == 'w':
        csv_writer.writeheader()
    return f, csv_writer


def _ensure_bom(output_file):
    """确保文件带有UTF-8 BOM"""
    with open(output_file, 'rb') as bf:
        has_bom = bf.read(3) == b'\xef\xbb\xbf'
    if not has_bom:
        with open(output_file, 'rb') as bf:
            content = bf.read()
        with open(output_file, 'wb') as bf:
            bf.write(b'\xef\xbb\xbf')
            bf.write(content)


def _retry_file_operation(operation, max_retries=5, initial_delay=2):
    """通用文件操作重试封装（处理文件被占用）"""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return operation()
        except PermissionError as e:
            last_err = e
            if attempt < max_retries:
                log.warning(f"文件操作被拒绝（第{attempt}/{max_retries}次重试）"
                            f"，{attempt * initial_delay}秒后重试...（请关闭Excel等程序）")
                time.sleep(attempt * initial_delay)
            else:
                log.error(f"文件操作失败（已达最大重试次数）: {e}")
                raise PermissionError(f"文件被占用，无法写入: {e}") from e
        except Exception as e:
            raise
    raise last_err


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
            time.sleep(3)
            captcha_found, _ = check_captcha(dp)
            if not captcha_found:
                return True
        log.warning(f"CAPTCHA timeout ({CAPTCHA_TIMEOUT}s), continuing")
        return True
    else:
        return True


def _get_company_href(card):
    """从卡片中提取公司href"""
    try:
        card_links = card.eles('css:a[href*="/gongsi/"]', timeout=2)
        for cl in card_links:
            href = cl.attr('href') or ''
            if '/gongsi/' in href and 'ka=' not in href and 'from=' not in href:
                return href
        if card_links:
            return card_links[0].attr('href')
    except:
        pass
    return None


def _get_company_name_from_panel(dp):
    """从右侧面板获取公司名称"""
    try:
        company_el = dp.ele('css:.job-company .company-name', timeout=2)
        if not company_el:
            company_el = dp.ele('css:.boss-info-company', timeout=2)
        if company_el:
            return company_el.text.strip()
    except:
        pass
    return ''


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

    # 岗位名称
    try:
        el = card.ele('css:.job-name', timeout=2)
        if el:
            info['GangWeiMingCheng'] = el.text.strip()
    except Exception:
        pass

    # 薪资（需要解码kanzhun混排字体的PUA字符）
    try:
        el = card.ele('css:.job-salary', timeout=2)
        if el:
            salary_text = el.text.strip()
            if not salary_text or all(ord(c) > 0x1FFFF for c in salary_text):
                salary_text = (el.attr('title') or '').strip()
            # 始终尝试解码PUA字符（BOSS薪资经常混合编码）
            decoded = _decode_kanzhun_salary(salary_text)
            if decoded and decoded != salary_text:
                info['XinChou'] = decoded
            else:
                info['XinChou'] = salary_text
    except Exception:
        pass

    # 公司名称
    try:
        el = card.ele('css:.boss-name', timeout=2)
        if el:
            info['GongSiMingCheng'] = el.text.strip()
    except Exception:
        pass

    # 城市.区域.商圈
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

    # 经验/学历
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

    # 技能标签
    try:
        skill_elems = card.eles('css:.tag-list > span', timeout=2)
        if skill_elems:
            info['JiNengBiaoQian'] = ','.join(s.text.strip() for s in skill_elems if s.text.strip())
    except Exception:
        pass

    # 福利标签
    try:
        welfare_elems = card.eles('css:.welfare-tag', timeout=2)
        if not welfare_elems:
            welfare_elems = card.eles('css:.tag', timeout=2)
        if welfare_elems:
            info['FuLiBiaoQian'] = ','.join(w.text.strip() for w in welfare_elems if w.text.strip())
    except Exception:
        pass

    return info




def _clean_job_detail(text):
    """清洗岗位详情文本，移除BOSS平台噪音内容。

    清洗内容：
    - App下载提示：去App、与BOSS随时沟通、前往App
    - BOSS/kanzhun字体混淆残留
    - 地址/位置等UI元素
    - 混入详情文本的发布人信息
    - 混入职位的标签（远程办公等）
    - Unicode私密区域（PUA）字符（kanzhun字体混淆）
    - Kangxi部首被混淆显示为普通汉字（kanzhun字体混淆）
    - kanzhun字体在词中间插入的混淆字符（'直聘'/'来自'）
    - 连续空白符
    """
    if not text:
        return ''

    # 移除kanzhun字体混淆的PUA字符（U+E000 - U+F8FF范围）
    text = re.sub(r'[\uE000-\uF8FF]', '', text)

    # 将kanzhun字体混淆的Kangxi部首还原为普通汉字
    # U+2F2F->工, U+2F45->方, U+2F47->日（从真实数据调试中确认）
    if any(chr(0x2F00) <= c <= chr(0x2FD5) for c in text):
        _KR = {'\u2F2F': '工', '\u2F45': '方', '\u2F47': '日'}
        text = text.translate(str.maketrans(_KR))

    # 移除kanzhun字体混淆在词中间插入的'直聘'和'来自'
    # 这些字符被kanzhun字体插入到词中间破坏词语，如：
    # 工作周期 -> 工直聘作周期, 长期兼职 -> 长期来自兼职,
    # 无要求 -> 无要直聘求, 每周工期 -> 每来自周工期
    # 这两个字符串只在混淆插入点出现，不会作为独立合法词出现
    text = text.replace('直聘', '').replace('来自', '')

    # 移除App下载/聊天提示（精确匹配）
    for p in ('去App', '与BOSS随时沟通', '前往App', '立即沟通',
              '微信扫码', '点击查看地图', '查看更多信息',
              '来自BOSS直聘', 'boss直聘', 'BOSS直聘', 'kanzhun', 'Kanzhun'):
        text = text.replace(p, '')

    # 移除单独的boss/kanzhun
    text = re.sub(r'\bboss\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\bkanzhun\b', '', text, flags=re.IGNORECASE)

    # 移除后面紧跟汉字的boss（混淆残留，如"boss工作周期"）
    text = re.sub(r'boss(?=[\u4e00-\u9fff])', '', text, flags=re.IGNORECASE)

    # 移除发布人信息及所有尾部UI元素
    text = re.sub(r'纪玉青 刚刚活跃.*$', '', text)
    text = re.sub(r'[\u4e00-\u9fff]{1,10}\s*(刚刚活跃|今日活跃|昨日活跃)\s*.*$', '', text)

    # 移除App操作按钮
    text = re.sub(r'去App', '', text)
    text = re.sub(r'与BOSS随时沟通', '', text)
    text = re.sub(r'前往App', '', text)

    # 移除远程办公标签
    text = re.sub(r'(?<=职位描述)\s*远程办公', '', text)
    text = re.sub(r'^远程办公\s*', '', text)

    # 移除剩余UI元素
    text = re.sub(r'查看更多信息.*$', '', text)
    text = re.sub(r'工作地址\s*[\u4e00-\u9fff0-9a-zA-Z·\-.,。]{3,100}$', '', text)
    text = re.sub(r'收藏\s*|\s*举报', '', text)

    # 合并空白符并清理
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[\s,，.。··]+$', '', text)

    return text


def _decode_kanzhun_salary(text):
    """解码BOSS直聘kanzhun混排字体中的PUA字符为真实数字。

    BOSS使用Unicode私密区域（PUA）字符U+E031到U+E03A来表示数字0-9。
    每个PUA字符对应一个数字。

    已确认的映射关系：
    U+E031 -> 0, U+E033 -> 2, U+E034 -> 3, U+E035 -> 4,
    U+E036 -> 5, U+E037 -> 6, U+E038 -> 7, U+E039 -> 8, U+E03A -> 9

    同时处理E032（对应数字1）和E03B。
    转换公式：数字 = (字符码 - 0xE031) % 10（适用于E031-E03A范围）。
    """
    if not text:
        return ''
    result = []
    for c in text:
        cp = ord(c)
        # 处理PUA数字字符E031-E03A（对应数字0-9）
        if 0xE031 <= cp <= 0xE03A:
            digit = (cp - 0xE031) % 10
            result.append(str(digit))
        # 处理E03B（编码特殊字符或数字）
        elif cp == 0xE03B:
            # 根据规律，E03B对应数字1
            result.append('1')
        elif cp <= 0xFFFF:
            result.append(c)
    decoded = ''.join(result)
    return decoded

def _extract_detail_panel(dp, card):
    """点击职位卡片打开右侧面板，提取岗位详情+发布人信息。

    使用自然点击方式，让SPA自行处理右侧面板更新。
    """
    info = {}

    for attempt in range(3):
        try:
            # 点击职位卡片（Playwright actions.click，不导航，不滚屏）
            try:
                card.scroll.to_see()
                time.sleep(0.5)
                dp.actions.click(card)
            except Exception:
                pass
            # === 岗位详情：使用CSS选择器（比正则更可靠）===
            # 面板文本包含薪资（带PUA字符）、地点、要求+描述
            # 我们去掉前面的非描述部分（薪资+地点+要求）
            try:
                box_elem = dp.ele('css:.job-detail-box', timeout=3)
                if box_elem:
                    text = box_elem.text.strip()
                    if text:
                        # 找到"职位描述"标记，取其后的内容作为岗位描述
                        desc_marker = '职位描述'
                        if desc_marker in text:
                            idx = text.find(desc_marker)
                            clean_text = text[idx:]  # 从"职位描述"开始保留
                        else:
                            # 备用方案：去掉前100个字符（薪资+地点+要求）
                            clean_text = text[100:]
                        # 移除UI元素
                        for ui_pattern in ['收藏', '举报', '微信扫码', '立即沟通']:
                            clean_text = clean_text.replace(ui_pattern, '')
                        # 移除BOSS直聘/kanzhun/boss字体混淆残留
                        clean_text = re.sub(r'BOSS直聘', '', clean_text)
                        clean_text = re.sub(r'kanzhun', '', clean_text, flags=re.IGNORECASE)
                        # 移除汉字前面插入的boss（如"boss工作周期" -> "b工作周期"）
                        clean_text = re.sub(r'[a-zA-Z]boss(?=[\u4e00-\u9fff])', lambda m: m.group()[0], clean_text, flags=re.IGNORECASE)
                        # 移除单独的boss/kanzhun
                        clean_text = re.sub(r'\bkanzhun\b', '', clean_text, flags=re.IGNORECASE)
                        clean_text = re.sub(r'\bboss\b', '', clean_text, flags=re.IGNORECASE)
                        # 合并空白符
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        # 执行综合清洗
                        clean_text = _clean_job_detail(clean_text)
                        info['GangWeiXiangQing'] = clean_text[:5000]
            except Exception:
                pass

            # === 薪资：使用CSS选择器+PUA解码 ===
            try:
                sal_el = dp.ele('css:.job-salary', timeout=3)
                if sal_el:
                    raw_text = (sal_el.text or '').strip()
                    if raw_text:
                        decoded = _decode_kanzhun_salary(raw_text)
                        if decoded:
                            info['XinChou'] = decoded
                    # 备用：尝试从title属性获取
                    if not info.get('XinChou'):
                        title = sal_el.attr('title')
                        if title:
                            decoded = _decode_kanzhun_salary(title.strip())
                            if decoded:
                                info['XinChou'] = decoded
            except Exception:
                pass

            # === 发布人信息：使用CSS选择器（有多级备用） ===
            panel = dp.ele('css:.job-boss-info', timeout=3)
            if panel:
                # 优先提取活跃状态（在提取姓名之前，因为name元素包含状态文本）
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

                # 发布人姓名：<h2 class="name"> 同时包含姓名和活跃状态
                # 需要从完整文本中提取纯姓名部分
                for sel in ['css:.name', 'css:h2.name', 'css:h2']:
                    try:
                        name_el = panel.ele(sel, timeout=2)
                        if name_el:
                            # 获取完整文本（含活跃状态）
                            name_text = name_el.text.strip()
                            if name_text:
                                # h2.name元素如"陈女士 在线"，需要只保留姓名部分
                                # 去掉常见状态后缀
                                name_text = re.sub(r'\s+(在线|刚刚活跃|已读|1分钟前|5分钟前|今日活跃|昨日活跃).*$', '', name_text)
                                name_text = re.sub(r'\s*[·•·]\s*(在线|活跃|已读).*$', '', name_text)
                                if name_text.strip():
                                    info['FaBuRenMingCheng'] = name_text.strip()
                                    break
                    except Exception:
                        continue

                # HR职称：格式为"公司 · 职称"
                for sel in ['css:.boss-info-attr', 'css:.boss-title', 'css:.title']:
                    try:
                        attr_el = panel.ele(sel, timeout=2)
                        if attr_el:
                            attr_text = attr_el.text.strip()
                            if attr_text and ' · ' in attr_text:
                                parts = attr_text.split(' · ', 1)
                                # parts[0]=公司名, parts[1]=HR职称
                                if len(parts) > 1 and parts[1].strip():
                                    info['FaBuRenZhiCheng'] = parts[1].strip()
                                if parts[0].strip() and not info.get('GongSiMingCheng'):
                                    info['GongSiMingCheng'] = parts[0].strip()
                                break
                            elif attr_text:
                                # 无分隔符，可能是纯职称或公司名
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
            log.debug(f"提取详情面板第{attempt+1}次失败: {e}")
            time.sleep(1)

    return info



def _extract_company_fields_from_page(dp):
    """从公司详情页提取工商注册信息（纯DOM解析，不过API）。

    提取策略（优先级从高到低）：
    1. 正则全文搜索（最可靠，BOSS页面文本里必有这些字段）
    2. ka属性选择器（52pojie帖子揭示：BOSS用ka属性定位元素）
    3. CSS类名选择器（兜底）
    4. 信息列表结构化提取

    ka属性参考（52pojie帖子揭示的BOSS内部元素定位）：
    - job-detail-company_custompage: 公司名
    - job-detail-brandindustry: 行业
    - job-detail-stage: 融资阶段
    - job-detail-scale: 公司规模
    - 公司工商信息Tab里有：统一社会信用代码、法定代表人、注册资本等
    """
    info = {}

    # 获取页面全文（用于正则提取）
    all_text = None
    try:
        all_text = dp.text
    except:
        pass
    if not all_text:
        try:
            all_text = dp.run_js("return document.body.innerText")
        except:
            pass

    # ==================== 1. 正则全文提取（最可靠） ====================
    if all_text:
        log.debug(f"公司页文本长度: {len(all_text)}")

        regex_patterns = [
            # 字段名: 正则模式
            # 统一社会信用代码可能是18位数字+大写字母，也可能包含小写字母
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
            ('NaShuiRenBieHao', r'纳税人识别号[：:\s]*([A-Za-z0-9]{20,24})'),
            ('ZuZhiJiGouDaiMa', r'组织机构代码[：:\s]*([A-Za-z0-9]{8}-?[A-Za-z0-9]|[A-Za-z0-9]{9})'),
            ('YingYeZhiZhaoHao', r'营业执照[号]?[：:\s]*([A-Za-z0-9]{13,18})'),
        ]

        for field, pattern in regex_patterns:
            if field not in info or not info[field]:
                m = re.search(pattern, all_text)
                if m:
                    val = m.group(1).strip()
                    if val:
                        info[field] = val
                        log.debug(f"正则提取: {field} = {val}")
    else:
        log.debug("无法获取页面文本进行正则提取")

    # ==================== 2. ka属性选择器（52pojie揭示的BOSS内部定位方式） ====================
    ka_selectors = [
        ('job-detail-company_custompage', 'GongSiMingCheng'),
        ('job-detail-brandindustry', 'LingYu'),
        ('job-detail-stage', 'GongSiJieDuan'),
        ('job-detail-scale', 'GongSiGuiMo'),
    ]
    for ka_val, field_key in ka_selectors:
        if field_key in info and info[field_key]:
            continue
        try:
            sel = f'xpath://*[@ka="{ka_val}"]'
            el = dp.ele(sel, timeout=1)
            if el:
                val = el.text.strip()
                if val and len(val) < 100:
                    info[field_key] = val
                    log.debug(f"ka属性提取: {field_key} = {val}")
        except:
            pass

    # ==================== 3. CSS类名选择器兜底 ====================
    css_map = [
        ('GongSiMingCheng', ['css:.company-name', 'css:.info-header .name', 'css:.company-title .name',
                             'css:.base-info .company-name', 'css:.company-tab-info h1', 'css:h1']),
        ('FaDingDaiBiaoRen', ['css:.legal-person-name', 'css:.fr', 'css:.legalPerson']),
        ('ZhuCeZiBen', ['css:.registered-capital', 'css:.reg-capital', 'css:.capital']),
        ('ChengLiRiQi', ['css:.establish-date', 'css:.start-date', 'css:.found-date']),
        ('JingYingZhuangTai', ['css:.business-status', 'css:.company-status', 'css:.status']),
        ('GongSiLeiXing', ['css:.company-type', 'css:.company-nature', 'css:.nature']),
        ('GongSiGuiMo', ['css:.company-size', 'css:.company-scale', 'css:.scale', 'css:.size']),
        ('GongSiJieDuan', ['css:.company-stage', 'css:.financing-stage', 'css:.stage']),
        ('GongSiRenShu', ['css:.employee-count', 'css:.staff-count', 'css:.count']),
        ('GongSiJianJie', ['css:.company-intro', 'css:.company-desc', 'css:.company-about']),
        ('GongSiDiZhi', ['css:.company-address', 'css:.company-addr', 'css:.address']),
        ('GongSiGuanWang', ['css:.company-website', 'css:.website']),
    ]
    for field, selectors in css_map:
        if field in info and info[field]:
            continue
        for sel in selectors:
            try:
                el = dp.ele(sel, timeout=1)
                if el:
                    val = el.text.strip()
                    if val and len(val) > 1 and len(val) < 200:
                        skip_vals = ['公司规模', '融资阶段', '营业执照', '天眼查', '附近公司',
                                     '查看更多', 'Boss', 'BOSS', 'k', '职位', '招聘', '收藏']
                        if any(p in val for p in skip_vals):
                            continue
                        info[field] = val[:2000] if field == 'GongSiJianJie' else val[:500]
                        break
            except:
                continue

    # ==================== 4. 结构化信息列表提取（BOSS公司页标准布局） ====================
    try:
        list_selectors = [
            'css:.base-info tr',
            'css:.info-list .item',
            'css:.company-info .row',
            'css:.detail-info li',
            'css:.reg-info li',
        ]
        for list_sel in list_selectors:
            items = dp.eles(list_sel, timeout=2)
            if not items:
                continue
            for item in items:
                try:
                    t = item.text.strip()
                    if not t or len(t) < 5:
                        continue
                    for sep in ['：', ':', '——', '-', '─']:
                        if sep in t:
                            parts = t.split(sep, 1)
                            if len(parts) == 2:
                                label = parts[0].strip()
                                value = parts[1].strip()
                                if not value or len(value) < 1:
                                    continue
                                lm = {
                                    '统一社会信用代码': 'TongYiSheHuiXinYongDaiMa',
                                    '法定代表人': 'FaDingDaiBiaoRen',
                                    '注册资本': 'ZhuCeZiBen',
                                    '成立时间': 'ChengLiRiQi',
                                    '成立日期': 'ChengLiRiQi',
                                    '经营状态': 'JingYingZhuangTai',
                                    '企业类型': 'GongSiLeiXing',
                                    '公司类型': 'GongSiLeiXing',
                                    '公司规模': 'GongSiGuiMo',
                                    '公司阶段': 'GongSiJieDuan',
                                    '融资阶段': 'GongSiJieDuan',
                                    '员工人数': 'GongSiRenShu',
                                    '所属行业': 'LingYu',
                                    '公司地址': 'GongSiDiZhi',
                                    '注册地址': 'GongSiDiZhi',
                                    '公司官网': 'GongSiGuanWang',
                                }
                                if label in lm and (lm[label] not in info or not info[lm[label]]):
                                    info[lm[label]] = value[:500]
                                    log.debug(f"列表提取: {lm[label]} = {value}")
                            break
                except:
                    continue
            if any(info.get(v) for v in info):
                break
    except Exception as e:
        log.debug(f"结构化列表提取异常: {e}")

    # 后处理：清理提取值中的换行符和尾部噪音
    for key in info:
        if info[key]:
            # 移除换行符和多余空白
            info[key] = re.sub(r'[\r\n\t]+', '', info[key])
            # 移除常见的尾部噪音
            info[key] = re.sub(r'[\s,，.。]*(收藏|举报|分享|立即沟通|查看更多).*$', '', info[key])
            info[key] = info[key].strip()

    log.debug(f"公司页最终提取字段: {[(k, (v[:50] if v else '')) for k, v in info.items()]}")
    return info

def _is_at_bottom(dp):
    try:
        scroll_y = dp.run_js('return window.scrollY')
        scroll_h = dp.run_js('return document.documentElement.scrollHeight')
        inner_h = dp.run_js('return window.innerHeight')
        return (scroll_y + inner_h) >= (scroll_h - 100)
    except Exception:
        return False


def scroll_and_collect(dp, keyword, max_detail=None):
    """采集所有可见卡片，实时检测新数据加载，直到列表末尾。

    流程：
    1. 点击卡片获取岗位信息
    2. 点击后检查DOM中卡片数量，没有下一个卡片则等待新数据
    3. 等待后仍无新数据则停止
    """
    jobs_list = []
    seen_keys = set()
    job_list_url = build_search_url(keyword)

    log.info(f"Start collecting: {keyword}")
    dp.get(job_list_url, timeout=30)
    time.sleep(3)

    if not wait_for_captcha(dp):
        log.warning("CAPTCHA not passed, may get partial data")

    cards = _get_job_cards(dp)
    if not cards:
        log.info("无卡片，停止采集")
        return jobs_list

    i = 0
    total_processed = 0
    while i < len(cards):
        if max_detail and total_processed >= max_detail:
            log.info(f"已达到本次最大处理数量 {max_detail}，停止采集")
            break

        card = cards[i]
        # 提取公司href（点击前就要拿到）
        company_href = _get_company_href(card)

        # 点击卡片触发详情面板
        try:
            card.scroll.to_see()
            dp.actions.click(card)
        except:
            pass

        # 提取岗位信息
        info = _extract_list_fields(card)
        info.update(_extract_detail_panel(dp, card))

        # 补充公司名称
        if not info.get('GongSiMingCheng'):
            info['GongSiMingCheng'] = _get_company_name_from_panel(dp)

        key = make_dedup_key(
            info.get('GongSiMingCheng', ''),
            info.get('GangWeiMingCheng', ''),
            info.get('ChengShi', '')
        )

        if key not in seen_keys:
            seen_keys.add(key)
            if company_href:
                info['_company_href'] = company_href
            jobs_list.append(info)
            total_processed += 1
            log.info(f"  [{i+1}/{len(cards)}] {info.get('GongSiMingCheng')} - {info.get('GangWeiMingCheng')}")

        # 只有当前卡片是当前cards列表的最后一条时，才检查是否加载了新数据
        if i == len(cards) - 1:
            current_count = len(dp.eles('css:.job-card-box', timeout=0))
            if current_count <= len(cards):
                dp.scroll.to_bottom()
                log.info("没有新数据，滚动到底部，重试一遍")
                time.sleep(1.5)
                current_count = len(dp.eles('css:.job-card-box', timeout=0))
                if current_count <= len(cards):
                    log.info("无新数据，列表已到末尾，停止采集")
                    break
            log.info(f"检测到新数据，加载后共 {current_count} 张卡片")
            cards = _get_job_cards(dp)

        i += 1

    if not jobs_list:
        log.warning("无数据")
    else:
        log.info(f"本次共采集 {len(jobs_list)} 条记录")

    return jobs_list


def main():
    global _global_csv_file, _global_csv_writer, _global_all_jobs

    log.info("=" * 70)
    log.info("BOSS直聘数据采集工具 v6.0".center(50))
    log.info("=" * 70)
    log.info(f"关键词: {config.SEARCH_QUERIES}")
    log.info(f"城市: {config.CITY_CODE}")
    log.info(f"职位类型: {'兼职' if config.JOB_TYPE == 'parttime' else '全职'}")

    existing_keys = set()
    total_previous = 0
    if INCREMENTAL_MODE:
        existing_keys = load_existing_records(config.BOSS_OUTPUT_FILE)
        total_previous = len(existing_keys)
        if total_previous > 0:
            log.info(f"发现 {total_previous} 条历史记录")

    csv_mode = ('a' if INCREMENTAL_MODE and os.path.exists(config.BOSS_OUTPUT_FILE) else 'w')
    _global_csv_file, _global_csv_writer = create_csv(config.BOSS_OUTPUT_FILE, mode=csv_mode)

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
            user_data = chrome_user_data.rstrip('\\/')
            co.set_user_data_path(user_data)
            log.info(f"使用Chrome配置: {chrome_user_data}")

        chrome_browser_path = getattr(config, 'CHROME_BROWSER_PATH', '')
        if chrome_browser_path:
            co.set_browser_path(chrome_browser_path)

        dp = ChromiumPage(addr_or_opts=co)
        log.info("浏览器启动成功")

        dp.get("https://www.zhipin.com/web/geek/job", timeout=30)
        time.sleep(3)
        log.info(f"当前URL: {dp.url}")

    except Exception as e:
        log.error(f"浏览器启动失败: {e}")
        return

    all_jobs = []
    new_count = 0
    skip_count = 0

    try:
        for keyword in config.SEARCH_QUERIES:
            log.info(f"{'=' * 50}")
            log.info(f"正在采集关键词: {keyword}")
            log.info(f"{'=' * 50}")

            jobs = scroll_and_collect(dp, keyword, max_detail=config.MAX_DETAIL_COUNT)

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

            log.info(f"关键词'{keyword}': 采集到{len(jobs)}条记录, 新增{new_count}条")

            if all_jobs:
                for job_item in all_jobs:
                    _retry_file_operation(
                        lambda: _global_csv_writer.writerow(_job_to_csv_row(job_item))
                    )
                _safe_flush_csv()
                log.info(f"已写入{len(all_jobs)}条记录到CSV")
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
        log.warning("Ctrl+C中断 - 正在保存进度")

    finally:
        try:
            _global_csv_file.close()
            log.info("CSV文件已关闭")
        except Exception:
            pass
        try:
            dp.quit()
            log.info("浏览器已关闭")
        except Exception:
            pass

    log.info("=" * 50)
    log.info("采集完成")
    log.info(f"输出文件: {config.BOSS_OUTPUT_FILE}")
    log.info(f"新增记录: {new_count}条")
    if INCREMENTAL_MODE:
        log.info(f"历史记录: {total_previous}条, 跳过: {skip_count}条")
    log.info("=" * 50)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("程序被中断")
    except Exception as e:
        log.error(f"未处理的异常: {e}")
        import traceback
        traceback.print_exc()
