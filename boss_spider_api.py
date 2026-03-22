#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
BOSS 直聘数据采集脚本 v5.3 (Phase 3: 验证码智能检测版)
- 使用API拦截获取数据，更稳定
- 绕过字体加密获取真实薪资
- 支持增量更新，自动跳过已采集记录
- 统一日志模块（文件+控制台）
- 关键步骤重试机制
- 每阶段完成自动保存
"""

import csv
import time
import json
import re
import os
import signal
import sys

import DrissionPage
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Settings, Actions

import config
import logger as log_module
from logger import get_logger
from utils import retry_on_failure, flush_csv

Settings.set_singleton_tab_obj(False)

# ==================== 增量更新配置 ====================
INCREMENTAL_MODE = getattr(config, 'INCREMENTAL_MODE', True)
PROGRESS_FILE = 'boss_progress.json'

# ==================== 验证码配置 ====================
CAPTCHA_STRATEGY = getattr(config, 'CAPTCHA_STRATEGY', 'manual')
CAPTCHA_TIMEOUT = getattr(config, 'CAPTCHA_TIMEOUT', 60)
CAPTCHA_SELECTORS = getattr(config, 'CAPTCHA_SELECTORS', [
    '.geetest_radar_tip',
    '.geetest_widget',
    '.verify-container',
    '.geetest_panel',
    '.nc_wrapper',
    '#nc_1_n1z',
    '.verification-code',
])

# ==================== 日志模块 ====================
log = get_logger('boss_spider')

# ==================== 全局紧急保存状态 ====================
_global_csv_file = None
_global_csv_writer = None
_global_all_jobs = []


# ==================== 信号处理（Ctrl+C 优雅退出）====================

def _signal_handler(signum, frame):
    log.warning("收到中断信号，正在保存进度...")
    _emergency_save()
    sys.exit(0)


def _emergency_save():
    """紧急保存当前进度"""
    global _global_csv_file, _global_all_jobs
    if _global_csv_file:
        try:
            _global_csv_file.flush()
            os.fsync(_global_csv_file.fileno())
            log.info(f"紧急保存完成，{len(_global_all_jobs)} 条记录已落盘")
        except Exception as e:
            log.error(f"紧急保存失败: {e}")


# 注册信号处理器（仅 Windows）
if os.name == 'nt':
    try:
        signal.signal(signal.SIGINT, _signal_handler)
    except Exception:
        pass


# ==================== 工具函数 ====================

def normalize_company_name(name):
    """标准化公司名称（用于去重匹配）"""
    if not name:
        return ''
    name = re.sub(
        r'(有限公司|股份有限公司|责任有限公司|集团|科技|网络|信息技术|电子|'
        r'系统|集成|发展|控股|投资|咨询|管理|服务|教育|文化|传媒|环境|能源|'
        r'电力|新能源|智能|数据|软件|平台)$',
        '', name
    )
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'（[^）]*）', '', name)
    return name.strip()


def make_dedup_key(company_raw, job_title, city):
    """生成去重键"""
    company_std = normalize_company_name(company_raw)
    return f"{company_std}_{job_title}_{city}"


@retry_on_failure(max_retries=3, delay=1, on_retry='boss_spider')
def load_progress_state():
    """加载进度状态（带重试）"""
    if not os.path.exists(PROGRESS_FILE):
        return {}
    try:
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"加载进度文件失败: {e}")
        return {}


@retry_on_failure(max_retries=3, delay=1, on_retry='boss_spider')
def save_progress_state(state):
    """保存进度状态（带重试 + 立即刷盘）"""
    try:
        # 确保目录存在
        dir_path = os.path.dirname(PROGRESS_FILE)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())

        log.debug(f"进度状态已保存: {state.get('total_collected', 0)} 条")
    except Exception as e:
        log.error(f"保存进度状态失败: {e}")


def load_existing_records(csv_file):
    """加载已有记录，建立去重集合"""
    existing_keys = set()
    if not os.path.exists(csv_file):
        return existing_keys

    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('公司名称', '').strip()
                job = row.get('岗位名称', '').strip()
                city = row.get('城市', '').strip()
                if company and job:
                    key = make_dedup_key(company, job, city)
                    existing_keys.add(key)
        log.info(f"已加载历史数据 {len(existing_keys)} 条")
    except Exception as e:
        log.warning(f"读取已有数据失败: {e}")

    return existing_keys


def csv_fieldnames():
    """统一的CSV字段列表"""
    return [
        '公司名称', '岗位名称', '城市', '区域', '商圈',
        '薪资', '经验', '学历', '领域', '性质', '规模',
        '技能标签', '福利标签', '岗位详情', '发布日期',
        '发布人名称', '发布人职称', '发布人电话', '发布人活跃状态',
        '公司类型', '公司规模', '公司阶段', '公司人数',
        '公司简介', '公司地址', '公司官网',
    ]


def create_csv(output_file, mode='w'):
    """创建CSV（支持追加模式）"""
    # 确保目录存在
    dir_path = os.path.dirname(output_file)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    f = open(file=output_file, mode=mode, encoding='utf-8-sig', newline='')
    csv_writer = csv.DictWriter(f, fieldnames=csv_fieldnames())
    if mode == 'w':
        csv_writer.writeheader()
    return f, csv_writer


def build_search_url(keyword, page=None):
    """构建URL，滚动模式下不需要page参数"""
    city = config.CITY_CODE
    job_type = '1903' if config.JOB_TYPE == 'parttime' else '1902'
    if page:
        return (f'https://www.zhipin.com/web/geek/job?query={keyword}'
                f'&city={city}&jobType={job_type}&page={page}')
    else:
        return (f'https://www.zhipin.com/web/geek/job?query={keyword}'
                f'&city={city}&jobType={job_type}')


def extract_from_api_response(data, jobs_list):
    """从API响应中提取数据"""
    try:
        if isinstance(data, dict):
            zpgeek_data = data.get('zpgeek', {})
            if zpgeek_data and isinstance(zpgeek_data, dict):
                job_list_data = zpgeek_data.get('searchJobList', {})
                if job_list_data and isinstance(job_list_data, dict):
                    reslist = job_list_data.get('jobList', [])
                    for job in reslist:
                        # 提取公司详情字段
                        company_info = job.get('brandJobTagList', []) or []
                        company_type = ''
                        company_stage = ''
                        for tag in company_info:
                            if isinstance(tag, dict):
                                tag_name = tag.get('tagName', '')
                                if '上市' in tag_name:
                                    company_type = tag_name
                                elif '融资' in tag_name or '轮' in tag_name:
                                    company_stage = tag_name

                        # 提取福利标签
                        welfare_list = job.get('welfareTagList', []) or []
                        welfare_tags = ','.join(
                            w.get('tagName', '') if isinstance(w, dict) else str(w)
                            for w in welfare_list
                        )

                        # 提取发布日期
                        post_time = job.get('postTime', '') or job.get('lastLoginTime', '')

                        jobs_list.append({
                            '公司名称': job.get('brandName', ''),
                            '岗位名称': job.get('jobName', ''),
                            '城市': job.get('cityName', ''),
                            '区域': job.get('areaName', ''),
                            '商圈': job.get('bizName', ''),
                            '薪资': job.get('salaryDesc', ''),
                            '经验': job.get('expName', ''),
                            '学历': job.get('degreeName', ''),
                            '领域': job.get('industryName', ''),
                            '性质': job.get('financingStateName', ''),
                            '规模': job.get('scaleName', ''),
                            '技能标签': ','.join(str(s) for s in job.get('skillTagList', []) or []),
                            '发布人名称': job.get('bossName', ''),
                            '发布人职称': job.get('bossTitle', ''),
                            '发布人活跃状态': job.get('activeTimeDesc', ''),
                            '岗位详情': '',
                            '福利标签': welfare_tags,
                            '发布日期': post_time,
                            '发布人电话': '',
                            '公司类型': company_type,
                            '公司规模': job.get('scaleName', ''),
                            '公司阶段': company_stage,
                            '公司人数': '',
                            '公司简介': '',
                            '公司地址': '',
                            '公司官网': '',
                        })
                    log.debug(f"API解析出 {len(reslist)} 条数据")
    except Exception as e:
        log.warning(f"解析API数据失败: {e}")


@retry_on_failure(max_retries=3, delay=3, on_retry='boss_spider')
def _fetch_with_retry(dp, url, timeout=30):
    """带重试的页面访问"""
    dp.get(url, timeout=timeout)
    log.debug(f"访问页面: {url}")
    time.sleep(2)  # 等待页面稳定


def check_captcha(dp):
    """
    检测页面是否存在验证码元素

    Returns:
        tuple: (是否存在验证码, 第一个命中的选择器名称)
    """
    for selector in CAPTCHA_SELECTORS:
        try:
            elem = dp.ele(f'css:{selector}', timeout=2)
            if elem:
                log.debug(f"验证码检测命中: {selector}")
                return True, selector
        except Exception:
            continue
    return False, None


def wait_for_captcha(dp):
    """
    根据配置的策略等待验证码处理

    策略模式:
        manual - 检测到验证码后打印醒目提示，等待用户按 Enter 继续
        auto   - 检测到验证码后自动轮询等待其消失，最长 CAPTCHA_TIMEOUT 秒
        skip   - 超时后跳过（返回 False）

    Returns:
        bool: True = 验证码已通过 / False = 超时跳过
    """
    if CAPTCHA_STRATEGY == 'manual':
        return _wait_manual(dp)
    elif CAPTCHA_STRATEGY == 'auto':
        return _wait_auto(dp)
    elif CAPTCHA_STRATEGY == 'skip':
        return _wait_skip(dp)
    else:
        log.warning(f"未知的 CAPTCHA_STRATEGY: {CAPTCHA_STRATEGY}，使用 manual 模式")
        return _wait_manual(dp)


def _wait_manual(dp):
    """手动模式：检测到验证码后等待用户按 Enter 继续"""
    # 先检查是否真的有验证码，避免误报
    captcha_found, selector = check_captcha(dp)
    if not captcha_found:
        log.debug("无验证码，继续执行")
        return True

    log.warning("=" * 50)
    log.warning("  [WARNING]  检测到验证码，请手动在浏览器中完成验证  [WARNING]")
    log.warning("=" * 50)
    log.info("完成后按 Enter 键继续...")
    try:
        input()  # 阻塞等待用户按 Enter
        log.info("继续执行...")
        return True
    except (EOFError, KeyboardInterrupt):
        log.warning("用户中断")
        return False


def _wait_auto(dp):
    """自动模式：轮询等待验证码消失"""
    # 先检查是否真的有验证码，避免误报
    captcha_found, selector = check_captcha(dp)
    if not captcha_found:
        log.debug("无验证码，继续执行")
        return True

    log.warning("=" * 50)
    log.warning("  [WARNING]  检测到验证码，自动等待验证通过  [WARNING]")
    log.warning("=" * 50)

    start_time = time.time()
    poll_interval = 5  # 每5秒检测一次

    while True:
        elapsed = time.time() - start_time
        remaining = CAPTCHA_TIMEOUT - elapsed

        if remaining <= 0:
            log.warning(f"验证码等待超时（{CAPTCHA_TIMEOUT}秒），继续执行（可能只采集到部分数据）")
            return True  # 超时后继续，不中断

        # 检测验证码是否还在
        captcha_found, selector = check_captcha(dp)
        if not captcha_found:
            log.info("验证码已通过，继续执行...")
            return True

        # 打印倒计时
        log.warning(f"验证码仍在显示，剩余 {int(remaining)} 秒后超时...")
        time.sleep(min(poll_interval, remaining))

    return False


def _wait_skip(dp):
    """跳过模式：超时后继续"""
    # 先检查是否真的有验证码，避免误报
    captcha_found, selector = check_captcha(dp)
    if not captcha_found:
        log.debug("无验证码，继续执行")
        return True

    log.warning("=" * 50)
    log.warning("  [WARNING]  检测到验证码，超时后继续  [WARNING]")
    log.warning("=" * 50)

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        remaining = CAPTCHA_TIMEOUT - elapsed

        if remaining <= 0:
            log.warning(f"验证码等待超时（{CAPTCHA_TIMEOUT}秒），继续执行（可能只采集到部分数据）")
            return True  # 超时后继续，不中断翻页

        captcha_found, selector = check_captcha(dp)
        if not captcha_found:
            log.info("验证码已通过，继续执行...")
            return True

        log.warning(f"验证码仍在显示，剩余 {int(remaining)} 秒后继续...")
        time.sleep(5)


def api_listen_mode(dp, keyword):
    """
    滚动加载模式：通过滚动左侧岗位列表触发懒加载，无需翻页
    """
    global _global_csv_file, _global_csv_writer, _global_all_jobs

    jobs_list = []

    log.info(f"启动滚动加载模式: {keyword}")

    # 获取滚动配置
    max_scrolls = getattr(config, 'MAX_SCROLLS', 5)
    log.info(f"最大滚动次数: {max_scrolls} 次")

    try:
        # 访问搜索页（无page参数）
        url = build_search_url(keyword, page=1)
        log.info(f"正在访问: {url}")
        dp.get(url, timeout=30)
        time.sleep(3)  # 等待初始数据加载

        # 验证码检查
        captcha_passed = wait_for_captcha(dp)
        if not captcha_passed:
            log.warning(f"验证码未通过，可能只采集到部分数据")

        # 初始提取
        page_jobs = _extract_jobs_from_dom(dp)
        log.info(f"初始加载: {len(page_jobs)} 条数据")
        jobs_list.extend(page_jobs)

        if len(page_jobs) == 0:
            log.warning("初始无数据，尝试备用DOM模式")
            dom_jobs = dom_mode(dp, keyword)
            return dom_jobs

        # 滚动加载：反复滚动左侧岗位列表，直到无新数据
        prev_count = len(page_jobs)
        no_new_count = 0  # 连续多少次滚动没有新数据

        for scroll_idx in range(1, max_scrolls + 1):
            # 滚动左侧岗位列表
            _scroll_job_list(dp)

            # 等待新数据加载
            time.sleep(2)

            # 提取当前所有卡片
            page_jobs = _extract_jobs_from_dom(dp)
            current_count = len(page_jobs)
            new_count = current_count - prev_count

            log.info(f"第 {scroll_idx} 次滚动: "
                     f"累计 {current_count} 条（+{new_count} 条新数据）")

            # 更新数据（用去重key避免重复）
            seen_keys = set()
            for job in jobs_list:
                seen_keys.add(make_dedup_key(job.get('公司名称',''), job.get('岗位名称',''), job.get('城市','')))

            for job in page_jobs:
                key = make_dedup_key(
                    job.get('公司名称', ''),
                    job.get('岗位名称', ''),
                    job.get('城市', '')
                )
                if key not in seen_keys:
                    jobs_list.append(job)
                    seen_keys.add(key)

            # 判断是否继续滚动
            if new_count <= 0:
                no_new_count += 1
                if no_new_count >= 2:
                    log.info(f"连续 {no_new_count} 次无新数据，停止滚动")
                    break
            else:
                no_new_count = 0
                prev_count = current_count

            # 到底了检查
            if current_count > 0 and _is_at_bottom(dp):
                log.info("已到达页面底部，停止滚动")
                break

        log.info(f"共获取 {len(jobs_list)} 条数据（{scroll_idx} 次滚动）")

    except Exception as e:
        log.error(f"提取异常: {e}")

    if not jobs_list:
        log.warning("滚动模式无数据，回退到 DOM 模式")
        jobs_list = dom_mode(dp, keyword)

    return jobs_list


def _scroll_job_list(dp):
    """
    滚动左侧岗位列表容器，触发懒加载
    BOSS直聘是左右分栏布局：左侧是岗位列表（可滚动），右侧是详情
    """
    # 方案1：滚动整个页面（会同时滚动左右两侧）
    dp.run_js('window.scrollBy(0, window.innerHeight)')
    time.sleep(0.5)

    # 方案2：尝试滚动左侧列表容器
    # 查找左侧岗位列表的滚动容器
    scroll_containers = [
        '.job-list-box',
        '.job-list-container',
        '[class*="job-list"]',
        '.job-card-box',
    ]

    for selector in scroll_containers:
        try:
            el = dp.ele(f'css:{selector}', timeout=2)
            if el and el.is_displayed():
                # 检查元素是否可以滚动
                scroll_height = dp.run_js('return arguments[0].scrollHeight', el)
                client_height = dp.run_js('return arguments[0].clientHeight', el)
                if scroll_height > client_height:
                    # 在元素内滚动
                    current_top = dp.run_js('return arguments[0].scrollTop', el)
                    dp.run_js('arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight', el)
                    new_top = dp.run_js('return arguments[0].scrollTop', el)
                    if new_top > current_top:
                        return  # 滚动成功
        except Exception:
            continue

    # 方案3：继续滚动整个页面
    dp.run_js('window.scrollBy(0, window.innerHeight)')
    time.sleep(0.5)


def _is_at_bottom(dp):
    """检查是否已滚动到页面底部"""
    try:
        scroll_y = dp.run_js('return window.scrollY')
        scroll_height = dp.run_js('return document.documentElement.scrollHeight')
        inner_height = dp.run_js('return window.innerHeight')
        # 底部误差容许100px
        return (scroll_y + inner_height) >= (scroll_height - 100)
    except Exception:
        return False


def _extract_jobs_from_dom(dp):
    """从当前页面DOM提取职位数据（新版BOSS直聘结构）"""
    jobs = []

    selectors = ['.job-card-wrap', '.job-card', '.job-list-box']
    cards = None
    for sel in selectors:
        try:
            cards = dp.eles(f'css:{sel}')
            if cards:
                break
        except Exception:
            continue

    if not cards:
        return jobs

    for card in cards:
        try:
            info = {
                '公司名称': '', '岗位名称': '', '城市': '',
                '区域': '', '商圈': '', '薪资': '', '经验': '',
                '学历': '', '领域': '', '性质': '', '规模': '',
                '技能标签': '', '福利标签': '', '岗位详情': '',
                '发布日期': '', '发布人名称': '', '发布人职称': '',
                '发布人电话': '', '发布人活跃状态': '',
                '公司类型': '', '公司规模': '', '公司阶段': '',
                '公司人数': '', '公司简介': '', '公司地址': '',
                '公司官网': '',
            }

            # 岗位名称
            try:
                el = card.ele('css:.job-title .job-name', timeout=2)
                info['岗位名称'] = el.text.strip() if el else ''
            except Exception:
                pass

            # 薪资
            try:
                el = card.ele('css:.job-salary', timeout=2)
                info['薪资'] = el.text.strip() if el else ''
            except Exception:
                pass

            # 公司/boss名称
            try:
                el = card.ele('css:.boss-name', timeout=2)
                info['公司名称'] = el.text.strip() if el else ''
            except Exception:
                pass

            # 城市/区域
            try:
                el = card.ele('css:.company-location', timeout=2)
                info['城市'] = el.text.strip() if el else ''
            except Exception:
                pass

            # 经验/学历
            try:
                tag_list = card.ele('css:.tag-list', timeout=2)
                if tag_list:
                    tags = [li.text.strip() for li in tag_list.eles('css:li', timeout=1) if li.text.strip()]
                    if len(tags) >= 1:
                        info['经验'] = tags[0]
                    if len(tags) >= 2:
                        info['学历'] = tags[1]
            except Exception:
                pass

            if info.get('岗位名称'):
                jobs.append(info)

        except Exception:
            continue

    return jobs


def dom_mode(dp, keyword):
    """DOM解析模式（增强字段提取）"""
    jobs_list = []

    log.info(f"DOM 解析模式: {keyword}")

    url = build_search_url(keyword)
    try:
        dp.get(url, timeout=30)
        time.sleep(5)
    except Exception as e:
        log.error(f"DOM 模式访问失败: {e}")
        return jobs_list

    for scroll_idx in range(config.MAX_SCROLLS):
        try:
            cards = None
            selectors = [
                '.job-card-wrap',
                '.job-card',
                '.job-list-box',
                'ul.rec-job-list > li',
            ]
            for sel in selectors:
                try:
                    cards = dp.eles(f'css:{sel}')
                    if cards:
                        log.debug(f"选择器 {sel} 命中 {len(cards)} 条")
                        break
                except Exception:
                    continue

            if not cards:
                log.debug(f"第 {scroll_idx + 1} 次滚动: 无匹配职位")

            for card in cards or []:
                try:
                    info = {
                        '公司名称': '', '岗位名称': '', '城市': '',
                        '区域': '', '商圈': '', '薪资': '', '经验': '',
                        '学历': '', '领域': '', '性质': '', '规模': '',
                        '技能标签': '', '福利标签': '', '岗位详情': '',
                        '发布日期': '', '发布人名称': '', '发布人职称': '',
                        '发布人电话': '', '发布人活跃状态': '',
                        '公司类型': '', '公司规模': '', '公司阶段': '',
                        '公司人数': '', '公司简介': '', '公司地址': '',
                        '公司官网': '',
                    }

                    # 提取岗位名称（标题）
                    try:
                        title_elem = card.ele('css:.job-title .job-name', timeout=2)
                        if title_elem:
                            info['岗位名称'] = title_elem.text.strip()
                        else:
                            # fallback: .job-title 整体文本
                            title_elem = card.ele('css:.job-title', timeout=2)
                            if title_elem:
                                info['岗位名称'] = title_elem.text.strip()
                    except Exception:
                        pass

                    # 提取薪资（BOSS直聘新版用 .job-salary）
                    try:
                        salary_elem = card.ele('css:.job-salary', timeout=2)
                        if salary_elem:
                            info['薪资'] = salary_elem.text.strip()
                    except Exception:
                        pass

                    # 提取公司/boss名称（新版在 .boss-name）
                    try:
                        company_elem = card.ele('css:.boss-name', timeout=2)
                        if company_elem:
                            info['公司名称'] = company_elem.text.strip()
                    except Exception:
                        pass

                    # 提取区域/城市（新版在 .company-location）
                    try:
                        loc_elem = card.ele('css:.company-location', timeout=2)
                        if loc_elem:
                            info['城市'] = loc_elem.text.strip()
                    except Exception:
                        pass

                    # 提取经验/学历要求（新版在 .tag-list li）
                    try:
                        tag_list = card.ele('css:.tag-list', timeout=2)
                        if tag_list:
                            tags = [li.text.strip() for li in tag_list.eles('css:li', timeout=1) if li.text.strip()]
                            if len(tags) >= 1:
                                info['经验'] = tags[0]
                            if len(tags) >= 2:
                                info['学历'] = tags[1]
                    except Exception:
                        pass

                    # 提取发布人名称（新版在 .boss-name）
                    try:
                        boss_elem = card.ele('css:.boss-name', timeout=2)
                        if boss_elem:
                            info['发布人名称'] = boss_elem.text.strip()
                    except Exception:
                        pass

                    # 提取技能标签
                    try:
                        tag_elems = card.eles('css:.tag-list > span', timeout=2)
                        if tag_elems:
                            info['技能标签'] = ','.join(t.text.strip() for t in tag_elems if t.text.strip())
                    except Exception:
                        pass

                    # 提取福利标签
                    try:
                        welfare_elems = card.eles('css:.welfare-tag', timeout=2)
                        if not welfare_elems:
                            welfare_elems = card.eles('css:.tag', timeout=2)
                        if welfare_elems:
                            info['福利标签'] = ','.join(w.text.strip() for w in welfare_elems if w.text.strip())
                    except Exception:
                        pass

                    if info['岗位名称']:
                        jobs_list.append(info)
                except Exception as e:
                    log.debug(f"解析卡片字段失败: {e}")
                    continue

            log.info(f"第 {scroll_idx + 1}/{config.MAX_SCROLLS} 次滚动: "
                     f"累计 {len(jobs_list)} 条")
            # 增量滚动：每次滚一屏，等待内容加载，再滚，再等
            # 这样能触发懒加载的分段触发点
            for _ in range(4):
                dp.run_js('window.scrollBy(0, window.innerHeight)')
                time.sleep(1)

        except Exception as e:
            log.warning(f"滚动失败: {e}")
            break

    log.info(f"DOM 模式完成: {len(jobs_list)} 条")
    return jobs_list


def _save_partial_results(csv_file, csv_writer, all_jobs, reason=''):
    """
    中间保存：当前关键词完成后、写盘前，先 flush 确认 CSV 结构正常
    """
    global _global_all_jobs
    try:
        csv_file.flush()
        os.fsync(csv_file.fileno())
        log.info(f"[保存] {reason} 当前 {len(_global_all_jobs)} 条记录已落盘")
    except Exception as e:
        log.error(f"[保存] flush 失败: {e}")


def check_login_status(dp):
    """
    检查是否已登录 BOSS 直聘
    已登录会跳转到 zpgeek 主页，未登录会跳转到登录页
    """
    try:
        current_url = dp.url
        if 'login' in current_url or '/user/' in current_url:
            log.warning(f"检测到未登录，当前 URL: {current_url}")
            return False
        log.info(f"登录状态正常，当前 URL: {current_url}")
        return True
    except Exception as e:
        log.warning(f"登录状态检测异常: {e}")
        return False


def main():
    global _global_csv_file, _global_csv_writer, _global_all_jobs

    log.info("=" * 70)
    log.info("BOSS 直聘数据采集 v5.3 - Phase 3".center(50))
    log.info("=" * 70)
    log.info(f"关键词: {config.SEARCH_QUERIES}")
    log.info(f"城市代码: {config.CITY_CODE}")
    log.info(f"增量模式: {'开启' if INCREMENTAL_MODE else '关闭'}")
    log.info(f"验证码策略: {CAPTCHA_STRATEGY} (超时 {CAPTCHA_TIMEOUT}秒)")
    log.info(f"日志级别: {getattr(config, 'LOG_LEVEL', 'DEBUG')}")

    # ==================== 增量更新初始化 ====================
    existing_keys = set()
    total_previous = 0

    if INCREMENTAL_MODE:
        existing_keys = load_existing_records(config.BOSS_OUTPUT_FILE)
        total_previous = len(existing_keys)
        if total_previous > 0:
            log.info(f"发现历史数据 {total_previous} 条，将跳过重复记录")
        log.info(f"进度状态文件: {PROGRESS_FILE}")

    # 追加模式打开 CSV
    csv_mode = ('a' if INCREMENTAL_MODE
                and os.path.exists(config.BOSS_OUTPUT_FILE)
                else 'w')
    _global_csv_file, _global_csv_writer = create_csv(
        config.BOSS_OUTPUT_FILE, mode=csv_mode
    )

    # ==================== 启动浏览器 ====================
    log.info("启动浏览器...")
    try:
        chrome_user_data = getattr(config, 'CHROME_USER_DATA_PATH', '')

        if chrome_user_data:
            # 使用 Chrome 配置文件（保持登录状态）
            log.info(f"使用 Chrome 配置文件: {chrome_user_data}")
            # 保持原路径不变（包含 Default），DrissionPage 直接使用
            user_data = chrome_user_data.rstrip('\\/')
            co = ChromiumOptions()
            co.set_user_data_path(user_data)
            # 显式指定 chrome 程序路径，避免 DrissionPage 找到 CentBrowser 等其他chromium内核浏览器
            chrome_browser_path = getattr(config, 'CHROME_BROWSER_PATH', '')
            if chrome_browser_path:
                co.set_browser_path(chrome_browser_path)
                log.info(f"使用指定浏览器: {chrome_browser_path}")
            dp = ChromiumPage(addr_or_opts=co)
            log.info("浏览器启动成功（配置文件模式）")

            # 先跳转到BOSS直聘网站，让Cookie生效，再检查登录状态
            log.info("正在加载BOSS直聘网站...")
            dp.get("https://www.zhipin.com/web/geek/job", timeout=30)
            time.sleep(3)
            log.info(f"当前 URL: {dp.url}")

            # 检查登录状态
            if not check_login_status(dp):
                log.warning("=" * 60)
                log.warning("  [WARNING]  检测到未登录，请在浏览器中手动登录 BOSS 直聘  [WARNING]")
                log.warning("  登录后按 Enter 键继续程序执行...")
                log.warning("=" * 60)
                try:
                    input()
                    log.info("继续执行...")
                except (EOFError, KeyboardInterrupt):
                    log.warning("用户取消")
                    return
                # 登录后再次检查
                if not check_login_status(dp):
                    log.error("登录验证失败，请检查登录状态")
                    return
        else:
            # 使用匿名模式（默认行为）
            log.info("使用匿名模式启动浏览器")
            dp = ChromiumPage()
            log.info("浏览器启动成功（匿名模式）")

    except Exception as e:
        log.error(f"浏览器启动失败: {e}")
        return

    all_jobs = []
    new_count = 0
    skip_count = 0

    try:
        for keyword in config.SEARCH_QUERIES:
            # ---- 检查增量模式关键词进度 ----
            if INCREMENTAL_MODE:
                state = load_progress_state()
                completed = state.get('completed_keywords', [])
                if keyword in completed:
                    log.info(f"关键词 '{keyword}' 已完成，跳过")
                    continue

            log.info(f"{'=' * 50}")
            log.info(f"开始采集: {keyword}")
            log.info(f"{'=' * 50}")

            # 采集（优先 API，失败回退 DOM）
            jobs = api_listen_mode(dp, keyword)

            # ---- 去重过滤 ----
            for job in jobs:
                key = make_dedup_key(
                    job.get('公司名称', ''),
                    job.get('岗位名称', ''),
                    job.get('城市', '')
                )
                if INCREMENTAL_MODE and key in existing_keys:
                    skip_count += 1
                    continue
                existing_keys.add(key)
                all_jobs.append(job)
                new_count += 1

            log.info(f"关键词 '{keyword}': 获取 {len(jobs)} 条，"
                     f"新增 {new_count} 条（去重跳过 {skip_count} 条）")

            # ---- 每关键词完成后立即写入 CSV（自动保存）----
            if all_jobs:
                log.info(f"写入 {len(all_jobs)} 条到 CSV...")
                for job_item in all_jobs:
                    _global_csv_writer.writerow(job_item)
                _global_csv_file.flush()
                os.fsync(_global_csv_file.fileno())
                log.info(f"写入完成，当前合计 {len(all_jobs)} 条")
                all_jobs = []  # 清空已写入

            # ---- 保存进度状态 ----
            # 只有实际采集到数据才标记为完成（避免0数据时关键词被误标记）
            if INCREMENTAL_MODE and new_count > 0:
                state = load_progress_state()
                if 'completed_keywords' not in state:
                    state['completed_keywords'] = []
                if keyword not in state['completed_keywords']:
                    state['completed_keywords'].append(keyword)
                state['last_keyword'] = keyword
                state['total_collected'] = new_count
                save_progress_state(state)
                log.debug(f"进度状态已更新")

    except KeyboardInterrupt:
        log.warning("收到 Ctrl+C，正在保存进度...")
        _emergency_save()
        return

    except Exception as e:
        log.error(f"运行异常: {e}")
        # 异常时也尝试保存
        if all_jobs:
            log.info("异常保存：写入剩余数据...")
            for job_item in all_jobs:
                _global_csv_writer.writerow(job_item)
            _global_csv_file.flush()
            os.fsync(_global_csv_file.fileno())
        raise

    finally:
        # ---- 关闭 CSV ----
        try:
            _global_csv_file.close()
            log.info("CSV 文件已关闭")
        except Exception:
            pass

        # ---- 关闭浏览器 ----
        try:
            dp.quit()
            log.info("浏览器已关闭")
        except Exception:
            pass

    # ==================== 汇总 ====================
    log.info("=" * 50)
    log.info("采集完成")
    log.info(f"输出文件: {config.BOSS_OUTPUT_FILE}")
    log.info(f"本次新增: {new_count} 条")
    if INCREMENTAL_MODE:
        log.info(f"历史累计: {total_previous} 条")
        log.info(f"本次去重跳过: {skip_count} 条")
        log.info(f"总计: {total_previous + new_count} 条")
    log.info("=" * 50)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("程序被中断")
    except Exception as e:
        log.error(f"未捕获的异常: {e}")
        import traceback
        traceback.print_exc()
