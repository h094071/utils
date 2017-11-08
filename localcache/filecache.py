#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
用本地文件做数据缓存装饰器（被装饰的函数参数不能是不可序列化的对象）
"""
import os
import json
import time
import logging
import hashlib
from functools import wraps
from lib.utils import force_utf8


def key_to_int(data):
    """
    字典的key转换成int
    :param data:
    :return:
    """
    if isinstance(data, dict):
        tmp_dict = {}
        for k, v in data.iteritems():
            try:
                tmp_dict.update({int(k): key_to_int(v)})
            except ValueError:
                tmp_dict.update({k: key_to_int(v)})
        return tmp_dict
    return data


class FileCache(object):
    """
    用本地文件做数据缓存
    """
    FILE_NAME_LIST = []

    def __init__(self, key, expire=86400, retry_count=50, interval=1.0):
        """
        初始化
        :param int expire: 失效时间, 分
        :param str key: 指定键值
        :param int start: 从开始计算的参数下标
        :param int retry_count: 重试次数
        :param float interval: 重试间隔, 单位: 秒
        :return: 装饰器
        """
        self.expire = expire
        self.retry_count = retry_count
        self.interval = interval
        self.key = key
        filename = key + ".json"
        self.filename = filename
        if not os.path.exists("/data/tmp"):
            os.mkdir("/data/tmp")

    def __call__(self, func):
        """
        文件缓冲
        :param args:
        :param kwargs:
        :return:
        """

        for i in xrange(self.retry_count):
            if self.filename in self.FILE_NAME_LIST:
                time.sleep(self.interval)
                logging.debug("cache file [%s] is using, operate file error", self.filename)
            else:
                logging.debug("cache file [%s] get succ", self.filename)
                can_use = True

        if not can_use:
            raise RuntimeError("cache file [%s] is using, operate file error" % self.filename)

        @wraps(func)
        def wrap_func(*args, **kwargs):
            self.FILE_NAME_LIST.append(self.filename)

            try:
                file_name = "/data/tmp/" + self.filename
                data_str = "{}"
                if kwargs.get("clear_cache"):
                    kwargs.pop("clear_cache")
                else:
                    # 不清除缓存时，读缓存
                    if os.path.exists(file_name):
                        with open(file_name, "r") as f:
                                data_str = f.read()
                cache_dict = json.loads(data_str)
                cache_params_str = cache_dict.get("param_str", "")
                param_str = self._gen_param_str(args, kwargs)

                # 函数参数相同并且在有效期内，缓冲可以使用。否则使用函数，缓冲结果
                if cache_params_str == param_str and time.time() - int(cache_dict["time_stamp"]) < self.expire:
                    cache_data = cache_dict["data"]
                    cache_data = force_utf8(cache_data)
                    cache_data = key_to_int(cache_data)
                    result = cache_data
                else:
                    result = func(*args, **kwargs)

                # 写缓冲
                with open(file_name, "w") as f:
                        cache_dict = {
                            "key": self.key,
                            "param_str": param_str,
                            "time_stamp": time.time(),
                            "data": result
                        }
                        f.write(json.dumps(cache_dict))
            except Exception as e:
                self.FILE_NAME_LIST.remove(self.filename)
                raise e

            self.FILE_NAME_LIST.remove(self.filename)
            return result

        return wrap_func

    def _gen_param_str(self, args, kwargs):
        """
        生成函数参数指纹
        :param list args: 位置参数列表
        :param dict kwargs: 关键字参数
        :return: str 函数参数指纹
        """
        args = args[1:]
        code = hashlib.md5()
        code.update(str(self.key))
        code.update("".join(sorted([str(i) for i in args])))
        code.update("".join(sorted([str(i) for i in kwargs.iteritems()])))
        return code.hexdigest()

    def clear_cache(self):
        """
        清除cache
        :return: None
        """
        file_name = "/data/tmp/" + self.filename
        self.FILE_NAME_LIST.append(self.filename)

        with open(file_name, "w") as file:
            file.write("{}")

        self.FILE_NAME_LIST.remove(self.filename)

if __name__ == "__main__":
    @FileCache("add")
    def add(a, b):
        print "no cache"
        return a + b

    c = add(1, 2)
