#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
用本地文件做数据缓存装饰器（被装饰的函数参数以及返回结果必须可序列化的对象）
"""
import fcntl
import os
import pickle
import time
import hashlib
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
        file_path = os.path.dirname(self.path)
        # 创建目录
        if not os.path.exists(file_path):
            os.makedirs(file_path)
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
            with open(self.path, "r") as fl:
                fd = fl.fileno()
                fcntl.lockf(fd, fcntl.LOCK_SH)
                cache_dict = self.read_file(fl)

            # 获得缓存数据
            param_hash = self._gen_param_hash(*args, **kwargs)
            data = self.get_cache_data(cache_dict, param_hash)
            if data:
                return data

            # 获得写锁
            with open(self.path, "r+") as fl:
                fd = fl.fileno()
                fcntl.lockf(fd, fcntl.LOCK_EX)
                # 获得写锁，查看文件是否已经变更（查看在进程阻塞获得锁的过程中缓存文件是否更新）
                cache_dict = self.read_file(fl)

                data = self.get_cache_data(cache_dict, param_hash)
                if data:
                    return data

                result = func(*args, **kwargs)
                # 写文件
                self.write_file(fl, result, param_hash)

                return result

        return wrap_func

    def _gen_param_hash(self, *args, **kwargs):
        """
        生成函数参数指纹
        :param list args: 位置参数列表
        :param list args: 位置参数列表
        :param dict kwargs: 关键字参数
        :return: str 函数参数指纹
        """
        args = args[1:]
        code = hashlib.md5()
        for i in args:
            if isinstance(i, list):
                i.sort()
        code.update("".join(sorted([str(i) for i in args])))
        code.update("".join(sorted([str(i) for i in kwargs.items()])))
        res = code.hexdigest()
        return res

    def read_file(self, fl):
        """
        读文件
        :param File fl:文件
        :return:
        """
        try:
            cache_dict = pickle.load(fl)
        except Exception:
            cache_dict = {}
        return cache_dict if cache_dict else {}

    def write_file(self, fl, data, param_hash):
        """
        写文件
        :param File fl: 文件
        :param data: 数据
        :param str params_hash: 参数指纹
        :return:
        """
        cache_dict = {
            "param_str": param_hash,
            "time_stamp": time.time(),
            "data": data
        }
        fl.seek(0, os.SEEK_SET)
        fl.truncate()
        pickle.dump(cache_dict, fl)
        fl.flush()  # 强制写回到文件

    def get_cache_data(self, cache_dict, params_hash):
        """
        获得数据
        :param dict cache_dict: 获得缓存数据
        :param str params_hash: 参数指纹
        :return:
        """
        cache_params_str = cache_dict.get("param_str", "")

        # 判断缓存是否可用
        if cache_params_str == params_hash and time.time() - cache_dict["time_stamp"] < self.expire:
            cache_data = cache_dict["data"]
            return cache_data
        else:
            return None

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
    class Test(object):
        @FileCache("tmp/add.txt")
        def add(self, a, b):
            print ("no cache")
            time.sleep(1)
            return a + b


    from multiprocessing import Process

    for i in range(5):
        print (" params %s" % i)
        for _ in range(10):
            p = Process(target=Test().add, args=(1, i))
            p.start()
            p.join()
