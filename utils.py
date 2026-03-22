#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
工具函数模块
包含重试装饰器、通用工具函数
"""

import time
import functools
import os
import csv

import config
from logger import get_logger

# 通用模块 logger
_log = get_logger('utils')


def retry_on_failure(max_retries: int = None, delay: float = None,
                      exceptions: tuple = (Exception,),
                      on_retry: str = None):
    """
    重试装饰器

    Args:
        max_retries: 最大重试次数（默认从 config 读取）
        delay: 重试间隔秒数（默认从 config 读取）
        exceptions: 需要捕获的异常类型元组
        on_retry: 重试时的额外日志消息（模块名）

    Usage:
        @retry_on_failure(max_retries=3, delay=2)
        def fetch_data():
            ...
    """
    if max_retries is None:
        max_retries = getattr(config, 'RETRY_MAX_RETRIES', 3)
    if delay is None:
        delay = getattr(config, 'RETRY_DELAY', 2)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            module = on_retry or 'utils'
            log = get_logger(module)
            last_exc = None

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_retries:
                        log.warning(
                            f"[{func.__name__}] 第 {attempt}/{max_retries} 次失败: {e}，"
                            f" {delay}s 后重试..."
                        )
                        time.sleep(delay)
                    else:
                        log.error(
                            f"[{func.__name__}] 全部 {max_retries} 次失败: {e}"
                        )
            # 全部失败后抛出异常
            raise last_exc

        return wrapper
    return decorator


def safe_write_csv(csv_writer, records, log_module='utils'):
    """
    安全写入 CSV（带重试）

    Args:
        csv_writer: csv.DictWriter 实例
        records: 要写入的记录列表
        log_module: 日志模块名
    """
    log = get_logger(log_module)

    @retry_on_failure(max_retries=3, delay=1, on_retry=log_module)
    def _write():
        for rec in records:
            csv_writer.writerow(rec)

    try:
        _write()
        log.debug(f"成功写入 {len(records)} 条记录")
    except Exception as e:
        log.error(f"写入 CSV 失败（已全部重试）: {e}")
        raise


def safe_read_file(file_path: str, mode='r', encoding='utf-8', log_module='utils'):
    """
    安全读取文件（带重试）

    Args:
        file_path: 文件路径
        mode: 读取模式
        encoding: 编码
        log_module: 日志模块名
    """
    log = get_logger(log_module)

    @retry_on_failure(max_retries=3, delay=1, on_retry=log_module)
    def _read():
        with open(file_path, mode, encoding=encoding) as f:
            return f.read()

    try:
        return _read()
    except Exception as e:
        log.error(f"读取文件 {file_path} 失败: {e}")
        return None


def safe_write_file(file_path: str, content: str,
                    mode='w', encoding='utf-8',
                    log_module='utils'):
    """
    安全写入文件（带重试）

    Args:
        file_path: 文件路径
        content: 内容
        mode: 写入模式
        encoding: 编码
        log_module: 日志模块名
    """
    log = get_logger(log_module)

    @retry_on_failure(max_retries=3, delay=1, on_retry=log_module)
    def _write():
        # 自动创建目录
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        with open(file_path, mode, encoding=encoding) as f:
            f.write(content)

    try:
        _write()
        log.debug(f"成功写入文件: {file_path}")
    except Exception as e:
        log.error(f"写入文件 {file_path} 失败: {e}")
        raise


def flush_csv(csv_file, csv_writer, all_jobs, log_module='boss_spider'):
    """
    立即刷新 CSV 到磁盘（紧急保存用）

    Args:
        csv_file: 文件对象
        csv_writer: csv.DictWriter
        all_jobs: 当前积累的职位列表
        log_module: 日志模块名
    """
    log = get_logger(log_module)
    try:
        csv_file.flush()
        os.fsync(csv_file.fileno())
        log.info(f"紧急保存完成，当前 {len(all_jobs)} 条记录已落盘")
    except Exception as e:
        log.error(f"紧急保存失败: {e}")
