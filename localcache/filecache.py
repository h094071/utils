#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
用本地文件做数据缓存装饰器（被装饰的函数参数以及返回结果必须可序列化的对象）
"""
import fcntl
import os
import pickle
import time
import logging
import hashlib
import six
import sys
from functools import wraps


class FileCache(object):
    """
    用本地文件做数据缓存
    """
    FILE_NAME_LOCK_LIST = []

    def __init__(self, path, expire=86400):
        """
        初始化
        :param int expire: 失效时间, 分
        :param str path: 缓存文件路径
        :return: 装饰器
        """
        self.expire = expire
        self.root = "/data/tmp/filecache/"
        self.path = os.path.join(self.root, path)
        dir = os.path.dirname(self.path)
        # 创建目录
        if not os.path.exists(dir):
            os.makedirs(dir)
        # 创建文件
        if not os.path.exists(self.path):
            with open(self.path, "w"):
                pass

    def __call__(self, func):
        """
        文件缓存
        :param function func: 被装饰的函数
        :return:
        """

        @wraps(func)
        def wrap_func(*args, **kwargs):

            # 获得读锁
            with open(self.path, "r") as file:
                fd = file.fileno()
                fcntl.lockf(fd, fcntl.LOCK_SH)
                try:
                    cache_dict = pickle.load(file)
                except Exception:
                    cache_dict = {}
            cache_dict = cache_dict if cache_dict else {}

            cache_params_str = cache_dict.get("param_str", "")
            param_str = self._gen_param_str(*args, **kwargs)

            # 判断缓存是否可用
            if cache_params_str == param_str and time.time() - int(cache_dict["time_stamp"]) < self.expire:
                cache_data = cache_dict["data"]
                result = cache_data
                return result
            else:
                # 获得写锁
                with open(self.path, "r+") as file:
                    fd = file.fileno()
                    fcntl.lockf(fd, fcntl.LOCK_EX)
                    # 获得写锁，查看文件是否已经变更（查看在进程阻塞获得锁的过程中缓存文件是否更新）
                    try:
                        cache_dict = pickle.load(file)
                    except Exception:
                        cache_dict = {}
                    cache_dict = cache_dict if cache_dict else {}
                    cache_params_str = cache_dict.get("param_str", "")

                    # 判断缓存是否可用
                    if cache_params_str == param_str and time.time() - int(cache_dict["time_stamp"]) < self.expire:
                        cache_data = cache_dict["data"]
                        result = cache_data
                        return result
                    else:
                        result = func(*args, **kwargs)
                        # 写文件
                        cache_dict = {
                            "param_str": param_str,
                            "time_stamp": time.time(),
                            "data": result
                        }
                        file.seek(0, os.SEEK_SET)
                        pickle.dump(cache_dict, file)
                        file.flush()  # 强制写回到文件
                        return result

        return wrap_func

    def _gen_param_str(self, *args, **kwargs):
        """
        生成函数参数指纹
        :param list args: 位置参数列表
        :param dict kwargs: 关键字参数
        :return: str 函数参数指纹
        """
        args = args[1:]
        code = hashlib.md5()
        code.update("".join(sorted([str(i) for i in args])))
        code.update("".join(sorted([str(i) for i in kwargs.items()])))
        res = code.hexdigest()
        return res

    def clear_cache(self):
        """
        清除cache
        :return: None
        """
        with open(self.path, "w") as file:
            fd = file.fileno
            fcntl.fcntl(fd, fcntl.LOCK_EX)
            file.write("")


if __name__ == "__main__":
    @FileCache("tmp/add.txt")
    def add(a, b):
        print ("no cache")
        time.sleep(1)
        return a + b


    from multiprocessing import Process

    for i in range(5):
        print (" params %s" % i)
        for _ in range(10):
            p = Process(target=add, args=(1, i))
            p.start()
            p.join()
