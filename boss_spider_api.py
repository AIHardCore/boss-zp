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


def build_search_url(keyword, page=1):
    """构建URL"""
    city = config.CITY_CODE
    job_type = '2' if config.JOB_TYPE == 'parttime' else '1'
    return (f'https://www.zhipin.com/web/geek/job?query={keyword}'
            f'&city={city}&jobType={job_type}&page={page}')


def extract_from_api_response(data, jobs_list):
    """从API响应中提取数据"""
    try:
        if isinstance(data, dict):
            if 'zpgeek/search/joblist' in str(data):
                reslist = (
                    data.get('zpgeek', {})
                    .get('searchJobList', {})
                    .get('jobList', [])
                )
                for job in reslist:
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
                        '技能标签': ','.join(job.get('skillTagList', [])),
                        '发布人名称': job.get('bossName', ''),
                        '发布人职称': job.get('bossTitle', ''),
                        '发布人活跃状态': job.get('activeTimeDesc', ''),
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
    log.warning("=" * 50)
    log.warning("  ⚠️  检测到验证码，请手动在浏览器中完成验证  ⚠️")
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
    log.warning("=" * 50)
    log.warning("  ⚠️  检测到验证码，自动等待验证通过  ⚠️")
    log.warning("=" * 50)

    start_time = time.time()
    poll_interval = 5  # 每5秒检测一次

    while True:
        elapsed = time.time() - start_time
        remaining = CAPTCHA_TIMEOUT - elapsed

        if remaining <= 0:
            log.error(f"验证码等待超时（{CAPTCHA_TIMEOUT}秒），跳过该关键词")
            return False

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
    """跳过模式：超时后跳过"""
    log.warning("=" * 50)
    log.warning("  ⚠️  检测到验证码，超时后跳过  ⚠️")
    log.warning("=" * 50)

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        remaining = CAPTCHA_TIMEOUT - elapsed

        if remaining <= 0:
            log.error(f"验证码等待超时（{CAPTCHA_TIMEOUT}秒），跳过该关键词")
            return False

        captcha_found, selector = check_captcha(dp)
        if not captcha_found:
            log.info("验证码已通过，继续执行...")
            return True

        log.warning(f"验证码仍在显示，剩余 {int(remaining)} 秒后跳过...")
        time.sleep(5)

    return False


def api_listen_mode(dp, keyword):
    """API拦截模式"""
    global _global_csv_file, _global_csv_writer, _global_all_jobs

    jobs_list = []

    log.info(f"启动 API 监听模式: {keyword}")

    try:
        dp.listen.start('zpgeek/search/joblist.json')
        log.debug("API 监听已启动")

        url = build_search_url(keyword)
        _fetch_with_retry(dp, url)

        # 验证码等待策略（固定等待改为智能检测）
        captcha_passed = wait_for_captcha(dp)
        if not captcha_passed:
            log.warning(f"验证码未通过，跳过关键词: {keyword}")
            return []

        for i in range(10):
            try:
                packet = dp.listen.wait(5)
                if packet:
                    data = packet.response.body
                    if data:
                        log.debug(f"收到 API 响应 #{i + 1}")
                        extract_from_api_response(data, jobs_list)
            except Exception as e:
                log.debug(f"等待数据包 #{i + 1}: {e}")
                break

        log.info(f"API 模式获取 {len(jobs_list)} 条数据")

    except Exception as e:
        log.error(f"API 监听异常: {e}")

    # API 模式无数据时回退 DOM 模式
    if not jobs_list:
        log.warning("API 模式无数据，回退到 DOM 模式")
        jobs_list = dom_mode(dp, keyword)

    return jobs_list


def dom_mode(dp, keyword):
    """DOM解析模式"""
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
                    try:
                        info['岗位名称'] = card.text.split('\n')[0] if card.text else ''
                    except Exception:
                        pass

                    if info['岗位名称']:
                        jobs_list.append(info)
                except Exception:
                    continue

            log.info(f"第 {scroll_idx + 1}/{config.MAX_SCROLLS} 次滚动: "
                     f"累计 {len(jobs_list)} 条")
            dp.scroll.to_bottom()
            time.sleep(2)

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
        dp = ChromiumPage()
        log.info("浏览器启动成功")
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
            if INCREMENTAL_MODE:
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
