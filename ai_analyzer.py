#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
大模型集成模块 - 支持多种 AI 服务
支持可配置 batch、分析进度、增量追加、错误隔离
"""

import os
import sys
import json
import csv
import time
from datetime import datetime

# 导入配置（放在最前面确保配置可用）
try:
    from config import (
        AI_BATCH_SIZE, AI_MODEL, AI_PROGRESS_STEP,
        AI_PROVIDER, AI_OUTPUT_FILE, AI_SAVE_INTERVAL,
        LOG_ENABLED, LOG_LEVEL, LOG_TO_FILE, LOG_TO_CONSOLE,
        LOG_FILE_DIR, LOG_FILE_PREFIX,
    )
except ImportError:
    # 旧版兼容：不依赖 config 时使用默认值
    AI_BATCH_SIZE = 20
    AI_MODEL = 'qwen-turbo'
    AI_PROGRESS_STEP = 5
    AI_PROVIDER = 'qwen'
    AI_OUTPUT_FILE = 'data_ai_analysis.json'
    AI_SAVE_INTERVAL = 10
    LOG_ENABLED = False

# 日志模块（延迟导入，避免循环依赖）
_logger = None

def _get_logger():
    global _logger
    if _logger is None:
        if LOG_ENABLED:
            try:
                from logger import get_logger
                _logger = get_logger('ai_analyzer')
            except Exception:
                _logger = _SimpleLogger()
        else:
            _logger = _SimpleLogger()
    return _logger

class _SimpleLogger:
    """无依赖的简单日志器（兼容旧版）"""
    def debug(self, msg, *args): print(f"[DEBUG] {msg % args}")
    def info(self, msg, *args): print(f"[INFO] {msg % args}")
    def warning(self, msg, *args): print(f"[WARN] {msg % args}")
    def error(self, msg, *args): print(f"[ERROR] {msg % args}")

# ==================== OpenAI 配置 ====================

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', 'your-api-key-here')
OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')

# 通义千问配置
QWEN_API_KEY = os.getenv('QWEN_API_KEY', 'your-api-key-here')

# 文心一言配置
ERNIE_API_KEY = os.getenv('ERNIE_API_KEY', 'your-api-key-here')
ERNIE_SECRET_KEY = os.getenv('ERNIE_SECRET_KEY', 'your-secret-key-here')


# ==================== 提示词模板 ====================

PROMPT_TEMPLATE = """你是一位资深的技术招聘专家和职业规划顾问。请分析以下 {count} 个职位描述，提取共性技术栈。

职位描述：
{job_descriptions}

请按以下格式输出：

1. **核心技术栈**（按重要性排序）
   - 编程语言：
   - 框架/库：
   - 数据库：
   - 工具/平台：

2. **技能要求等级**
   - 必须掌握（出现频率>70%）：
   - 优先掌握（出现频率30-70%）：
   - 加分项（出现频率<30%）：

3. **学习路线建议**
   - 第一阶段（基础）：
   - 第二阶段（进阶）：
   - 第三阶段（高级）：

4. **资源推荐**
   - 推荐学习资源（书籍、课程、文档）

请用中文回答，内容要具体、可操作。"""


# ==================== OpenAI 实现 ====================

def analyze_single_openai(job_descriptions, model=None):
    """使用 OpenAI 分析职位描述（单次调用）"""

    client = OpenAI(
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL
    )

    model = model or 'gpt-4o-mini'
    prompt = PROMPT_TEMPLATE.format(
        count=len(job_descriptions),
        job_descriptions=json.dumps(job_descriptions, ensure_ascii=False, indent=2)
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是一位资深的技术招聘专家和职业规划顾问。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=2000
    )

    return response.choices[0].message.content


# ==================== 通义千问实现 ====================

def analyze_single_qwen(job_descriptions, model=None):
    """使用通义千问分析职位描述（单次调用）"""

    try:
        import dashscope
        from dashscope import Generation
    except ImportError:
        raise ImportError("⚠ 请先安装通义千问 SDK: pip install dashscope")

    dashscope.api_key = QWEN_API_KEY

    model = model or 'qwen-turbo'
    prompt = PROMPT_TEMPLATE.format(
        count=len(job_descriptions),
        job_descriptions=json.dumps(job_descriptions, ensure_ascii=False, indent=2)
    )

    response = Generation.call(
        model=model,
        prompt=prompt
    )

    if response.status_code == 200:
        return response.output.text
    else:
        raise RuntimeError(f"通义千问调用失败: {response.message}")


# ==================== 文心一言实现 ====================

def analyze_single_ernie(job_descriptions, model=None):
    """使用文心一言分析职位描述（单次调用）"""

    import requests

    # 获取 access_token
    token_url = (f"https://aip.baidubce.com/oauth/2.0/token"
                 f"?grant_type=client_credentials&client_id={ERNIE_API_KEY}&client_secret={ERNIE_SECRET_KEY}")
    token_response = requests.get(token_url, timeout=10)
    access_token = token_response.json().get('access_token')

    if not access_token:
        raise RuntimeError("获取 access_token 失败")

    prompt = PROMPT_TEMPLATE.format(
        count=len(job_descriptions),
        job_descriptions=json.dumps(job_descriptions, ensure_ascii=False, indent=2)
    )

    url = (f"https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions"
           f"?access_token={access_token}")

    payload = json.dumps({
        "messages": [{"role": "user", "content": prompt}]
    })

    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=payload, timeout=30)

    if response.status_code == 200:
        result = response.json().get('result', '')
        if not result:
            raise RuntimeError("文心一言返回结果为空")
        return result
    else:
        raise RuntimeError(f"文心一言调用失败: {response.text}")


# ==================== 单次分析统一入口 ====================

def analyze_single(job_descriptions, provider=None, model=None):
    """
    使用指定 AI 服务单次分析一批职位描述

    Args:
        job_descriptions: 职位描述列表
        provider: AI 服务提供商 ('openai', 'qwen', 'ernie')
        model: 模型名称（可选）

    Returns:
        分析结果文本

    Raises:
        ImportError: 缺少依赖
        RuntimeError: API 调用失败
    """

    provider = (provider or AI_PROVIDER or 'qwen').lower()

    if provider == 'openai':
        return analyze_single_openai(job_descriptions, model)
    elif provider == 'qwen':
        return analyze_single_qwen(job_descriptions, model)
    elif provider == 'ernie':
        return analyze_single_ernie(job_descriptions, model)
    else:
        raise ValueError(f"不支持的 AI 服务: {provider}")


# ==================== 结果追加写入 ====================

def _load_existing_results(output_file):
    """加载已有分析结果，返回已分析的职位 key 集合"""
    if not os.path.exists(output_file):
        return {}, set()

    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            existing = {item.get('job_key'): item for item in data if item.get('job_key')}
            done_keys = set(existing.keys())
            return existing, done_keys
        elif isinstance(data, dict):
            # 兼容旧格式：{"results": [...]}
            results = data.get('results', [])
            existing = {item.get('job_key'): item for item in results if item.get('job_key')}
            done_keys = set(existing.keys())
            return existing, done_keys
    except (json.JSONDecodeError, IOError) as e:
        _get_logger().warning(f"加载已有结果失败，将覆盖: {e}")

    return {}, set()


def _save_results(output_file, results_dict, metadata):
    """保存分析结果到 JSON 文件（覆盖写入）"""
    all_results = list(results_dict.values())

    output_data = {
        'metadata': metadata,
        'results': all_results,
        'summary': {
            'total': len(all_results),
            'success': sum(1 for r in all_results if r.get('status') == 'success'),
            'failed': sum(1 for r in all_results if r.get('status') == 'failed'),
            'saved_at': datetime.now().isoformat(),
        }
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)


def _make_job_key(job):
    """生成职位的唯一标识 key"""
    parts = [
        job.get('company_name', ''),
        job.get('job_title', job.get('职位', '')),
        job.get('city', job.get('城市', '')),
    ]
    return '|'.join(str(p) for p in parts)


# ==================== 主分析函数（支持 batch + 进度 + 增量 + 错误隔离） ====================

def analyze_jobs(job_descriptions, batch_size=None, provider=None, model=None,
                 output_file=None, progress_step=None, save_interval=None,
                 show_progress=True):
    """
    使用 AI 批量分析职位描述

    Args:
        job_descriptions: 职位描述列表（每项为 dict）
        batch_size: 每批分析多少条（默认使用 AI_BATCH_SIZE）
        provider: AI 服务提供商（默认使用 AI_PROVIDER）
        model: 模型名称（默认使用 AI_MODEL）
        output_file: 结果输出文件（默认使用 AI_OUTPUT_FILE）
        progress_step: 每多少条显示一次进度（默认使用 AI_PROGRESS_STEP）
        save_interval: 每多少条保存一次（默认使用 AI_SAVE_INTERVAL）
        show_progress: 是否显示进度（默认 True）

    Returns:
        分析结果字典 {job_key: result_item}
    """

    batch_size = batch_size or AI_BATCH_SIZE
    provider = provider or AI_PROVIDER or 'qwen'
    model = model or AI_MODEL
    output_file = output_file or AI_OUTPUT_FILE
    progress_step = progress_step or AI_PROGRESS_STEP
    save_interval = save_interval or AI_SAVE_INTERVAL

    log = _get_logger()
    log.info(f"开始 AI 分析: {len(job_descriptions)} 条职位, batch_size={batch_size}, "
             f"provider={provider}, model={model}")

    # 加载已有结果（支持增量追加）
    existing_results, done_keys = _load_existing_results(output_file)
    if done_keys:
        log.info(f"发现 {len(done_keys)} 条已有分析结果，将跳过已分析条目")

    # 准备待分析列表
    pending = []
    for job in job_descriptions:
        key = _make_job_key(job)
        if key not in done_keys:
            pending.append(job)

    total_pending = len(pending)
    total_all = len(job_descriptions)
    log.info(f"本次需分析: {total_pending} 条 / 共 {total_all} 条")

    if total_pending == 0:
        log.info("没有需要分析的职位")
        return existing_results

    # 进度显示
    if show_progress:
        print(f"🔬 AI分析中... 0/{total_pending}")

    # 分批处理
    results = dict(existing_results)
    success_count = sum(1 for r in results.values() if r.get('status') == 'success')
    failed_count = sum(1 for r in results.values() if r.get('status') == 'failed')
    processed = 0

    for i in range(0, total_pending, batch_size):
        batch = pending[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_pending + batch_size - 1) // batch_size

        log.debug(f"处理 batch {batch_num}/{total_batches} ({len(batch)} 条)")

        try:
            # 单批次调用 AI
            job_texts = [j.get('job_description', j.get('描述', j.get('job_desc', ''))) for j in batch]
            analysis_result = analyze_single(job_texts, provider=provider, model=model)

            # 解析结果（每个 job 对应一条分析）
            # 由于 AI 返回的是整体分析，这里做简化：为每个 job 记录相同的分析结论
            # 实际场景中可让 AI 返回结构化 JSON 单独分析每条
            for job in batch:
                key = _make_job_key(job)
                results[key] = {
                    'job_key': key,
                    'company_name': job.get('company_name', job.get('公司名称', '')),
                    'job_title': job.get('job_title', job.get('职位', '')),
                    'city': job.get('city', job.get('城市', '')),
                    'analysis': analysis_result,
                    'status': 'success',
                    'analyzed_at': datetime.now().isoformat(),
                    'batch_num': batch_num,
                }
                success_count += 1

        except ImportError as e:
            log.error(f"Batch {batch_num} 缺少依赖: {e}")
            # 单条失败不影响其他
            for job in batch:
                key = _make_job_key(job)
                results[key] = {
                    'job_key': key,
                    'company_name': job.get('company_name', job.get('公司名称', '')),
                    'job_title': job.get('job_title', job.get('职位', '')),
                    'city': job.get('city', job.get('城市', '')),
                    'analysis': '',
                    'status': 'failed',
                    'error': str(e),
                    'analyzed_at': datetime.now().isoformat(),
                    'batch_num': batch_num,
                }
                failed_count += 1

        except Exception as e:
            log.error(f"Batch {batch_num} 分析失败: {e}")
            # 单条失败不影响其他
            for job in batch:
                key = _make_job_key(job)
                results[key] = {
                    'job_key': key,
                    'company_name': job.get('company_name', job.get('公司名称', '')),
                    'job_title': job.get('job_title', job.get('职位', '')),
                    'city': job.get('city', job.get('城市', '')),
                    'analysis': '',
                    'status': 'failed',
                    'error': str(e),
                    'analyzed_at': datetime.now().isoformat(),
                    'batch_num': batch_num,
                }
                failed_count += 1

        processed += len(batch)

        # 进度显示（按 progress_step 步进）
        remaining = total_pending - processed
        if show_progress and (processed % progress_step == 0 or remaining < progress_step):
            print(f"🔬 AI分析中... {processed}/{total_pending}")

        # 定期保存（防止中断丢失）
        if processed % save_interval == 0 and processed < total_pending:
            metadata = {
                'provider': provider,
                'model': model,
                'batch_size': batch_size,
                'total_input': total_all,
                'generated_at': datetime.now().isoformat(),
            }
            _save_results(output_file, results, metadata)
            log.debug(f"已保存中间结果: {len(results)} 条")

    # 最终保存
    metadata = {
        'provider': provider,
        'model': model,
        'batch_size': batch_size,
        'total_input': total_all,
        'total_analyzed': total_pending,
        'generated_at': datetime.now().isoformat(),
    }
    _save_results(output_file, results, metadata)

    # 完成提示
    if show_progress:
        print(f"✅ AI分析完成: 成功 {success_count} / 失败 {failed_count} / 共 {total_all} 条")
        print(f"📄 结果已保存到: {output_file}")

    log.info(f"AI分析完成: 成功 {success_count}, 失败 {failed_count}, 结果已保存到 {output_file}")

    return results


# ==================== 从 CSV 加载职位描述并分析 ====================

def analyze_from_csv(csv_file, batch_size=None, provider=None, model=None,
                      output_file=None, progress_step=None, save_interval=None,
                      max_rows=None, show_progress=True):
    """
    从 CSV 文件加载职位数据并进行分析

    Args:
        csv_file: CSV 文件路径
        batch_size: 每批分析多少条
        provider: AI 服务提供商
        model: 模型名称
        output_file: 结果输出文件
        progress_step: 每多少条显示一次进度
        save_interval: 每多少条保存一次
        max_rows: 最多读取多少条（用于测试）
        show_progress: 是否显示进度

    Returns:
        分析结果字典
    """

    log = _get_logger()
    log.info(f"从 CSV 加载职位数据: {csv_file}")

    if not os.path.exists(csv_file):
        log.error(f"CSV 文件不存在: {csv_file}")
        raise FileNotFoundError(f"CSV 文件不存在: {csv_file}")

    jobs = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if max_rows and i >= max_rows:
                    break
                jobs.append(row)
    except Exception as e:
        log.error(f"读取 CSV 失败: {e}")
        raise

    log.info(f"从 CSV 加载了 {len(jobs)} 条职位数据")

    return analyze_jobs(
        jobs,
        batch_size=batch_size,
        provider=provider,
        model=model,
        output_file=output_file,
        progress_step=progress_step,
        save_interval=save_interval,
        show_progress=show_progress,
    )


# ==================== 兼容旧接口 ====================

def analyze_with_openai(job_descriptions):
    """兼容旧接口：使用 OpenAI 分析（固定取前10条）"""
    return analyze_single(job_descriptions[:10], provider='openai')


def analyze_with_qwen(job_descriptions):
    """兼容旧接口：使用通义千问分析（固定取前10条）"""
    return analyze_single(job_descriptions[:10], provider='qwen')


def analyze_with_ernie(job_descriptions):
    """兼容旧接口：使用文心一言分析（固定取前10条）"""
    return analyze_single(job_descriptions[:10], provider='ernie')


def analyze_with_ai(job_descriptions, provider='openai'):
    """兼容旧接口：统一分析入口（固定取前10条）"""
    return analyze_single(job_descriptions[:10], provider=provider)


# ==================== CLI 入口 ====================

def main():
    import argparse

    parser = argparse.ArgumentParser(description='AI 职位分析工具')
    parser.add_argument('csv_file', nargs='?', help='CSV 文件路径（可选，不提供则进入测试模式）')
    parser.add_argument('--batch-size', type=int, dest='batch_size',
                        help=f'每批分析多少条（默认: {AI_BATCH_SIZE}）')
    parser.add_argument('--provider', default=AI_PROVIDER,
                        choices=['openai', 'qwen', 'ernie'],
                        help=f'AI 服务提供商（默认: {AI_PROVIDER}）')
    parser.add_argument('--model', default=AI_MODEL,
                        help=f'模型名称（默认: {AI_MODEL}）')
    parser.add_argument('--output', dest='output_file',
                        default=AI_OUTPUT_FILE,
                        help=f'结果输出文件（默认: {AI_OUTPUT_FILE}）')
    parser.add_argument('--progress-step', type=int, dest='progress_step',
                        default=AI_PROGRESS_STEP,
                        help=f'每多少条显示一次进度（默认: {AI_PROGRESS_STEP}）')
    parser.add_argument('--save-interval', type=int, dest='save_interval',
                        default=AI_SAVE_INTERVAL,
                        help=f'每多少条保存一次（默认: {AI_SAVE_INTERVAL}）')
    parser.add_argument('--max-rows', type=int, dest='max_rows',
                        help='最多读取多少条数据（用于测试）')
    parser.add_argument('--quiet', action='store_true',
                        help='静默模式，不显示进度')

    args = parser.parse_args()

    log = _get_logger()

    if args.csv_file:
        # CSV 模式
        results = analyze_from_csv(
            csv_file=args.csv_file,
            batch_size=args.batch_size,
            provider=args.provider,
            model=args.model,
            output_file=args.output_file,
            progress_step=args.progress_step,
            save_interval=args.save_interval,
            max_rows=args.max_rows,
            show_progress=not args.quiet,
        )
        log.info(f"分析完成，共 {len(results)} 条结果")
    else:
        # 测试模式
        print(f"""
╔══════════════════════════════════════════════════════╗
║           AI 职位分析工具 v2.0                         ║
╠══════════════════════════════════════════════════════╣
║  配置信息:                                             ║
║    AI_BATCH_SIZE    = {AI_BATCH_SIZE:<30}║
║    AI_MODEL         = {AI_MODEL:<30}║
║    AI_PROGRESS_STEP = {AI_PROGRESS_STEP:<30}║
║    AI_PROVIDER      = {AI_PROVIDER:<30}║
║    AI_OUTPUT_FILE   = {AI_OUTPUT_FILE:<30}║
║    AI_SAVE_INTERVAL = {AI_SAVE_INTERVAL:<30}║
╠══════════════════════════════════════════════════════╣
║  使用方式:                                             ║
║    python ai_analyzer.py <csv_file>                   ║
║    python ai_analyzer.py data_boss.csv                 ║
║    python ai_analyzer.py data_boss.csv --batch-size 20 ║
║    python ai_analyzer.py data_boss.csv --provider qwen║
╚══════════════════════════════════════════════════════╝
""")
        # 测试数据
        test_jobs = [
            {
                'company_name': '某科技公司',
                'job_title': 'AI Agent 开发工程师',
                'city': '北京',
                'job_description': '负责 AI Agent 开发，要求熟悉 Python、LangChain、OpenAI API，了解 RAG 技术'
            },
            {
                'company_name': '某互联网公司',
                'job_title': 'Python 后端工程师',
                'city': '上海',
                'job_description': '负责后端开发，要求熟悉 Python、Django、MySQL、Redis'
            }
        ]
        print(f"测试模式: 将分析 {len(test_jobs)} 条测试数据")
        results = analyze_jobs(test_jobs, show_progress=True)
        print(f"\n分析结果预览: {len(results)} 条")


# ==================== 测试 ====================

if __name__ == '__main__':
    main()
