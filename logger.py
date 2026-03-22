#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
统一日志模块
- 文件 + 控制台双输出
- 按日期分割日志文件
- 可配置日志级别
"""

import logging
import os
import sys
from datetime import datetime

import config


def get_logger(module_name: str) -> logging.Logger:
    """
    获取指定模块的日志记录器

    Args:
        module_name: 模块名称（用于日志格式中的 [模块] 标识）

    Returns:
        配置好的 Logger 实例
    """
    # 如果未启用日志，返回一个禁用状态的 logger
    if not getattr(config, 'LOG_ENABLED', True):
        logger = logging.getLogger(module_name)
        logger.disabled = True
        return logger

    logger = logging.getLogger(module_name)

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(getattr(config, 'LOG_LEVEL', 'DEBUG').upper())

    # 日志格式：[时间] [级别] [模块] 消息
    fmt = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # ---- 控制台输出 ----
    if getattr(config, 'LOG_TO_CONSOLE', True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

    # ---- 文件输出 ----
    if getattr(config, 'LOG_TO_FILE', True):
        log_dir = getattr(config, 'LOG_FILE_DIR', 'logs')
        log_prefix = getattr(config, 'LOG_FILE_PREFIX', 'boss_spider')
        today = datetime.now().strftime('%Y-%m-%d')

        # 自动创建日志目录
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f'{log_prefix}_{today}.log')
        file_handler = logging.FileHandler(
            log_file, mode='a', encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    # 防止传播到 root logger
    logger.propagate = False

    return logger


# ---- 快捷函数 ----

def debug(module_name: str, msg: str):
    get_logger(module_name).debug(msg)


def info(module_name: str, msg: str):
    get_logger(module_name).info(msg)


def warning(module_name: str, msg: str):
    get_logger(module_name).warning(msg)


def error(module_name: str, msg: str):
    get_logger(module_name).error(msg)
