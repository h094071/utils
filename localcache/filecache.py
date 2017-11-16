#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
用本地文件做数据缓存装饰器（被装饰的函数参数不能是不可序列化的对象）
"""
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
        self.path = "/data/tmp/filecache/"
        filename = key + ".data"
        self.filename = filename
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def __call__(self, func):
        """
        文件缓存
        :param function func: 被装饰的函数
        :return:
        """
        @wraps(func)
        def wrap_func(*args, **kwargs):
            # 获得锁
            can_use = False
            for i in xrange(self.retry_count):
                if self.filename in self.FILE_NAME_LOCK_LIST:
                    time.sleep(self.interval)
                    logging.debug("cache file [%s] is using, operate file error", self.filename)
                else:
                    logging.debug("cache file [%s] get succ", self.filename)
                    self.FILE_NAME_LOCK_LIST.append(self.filename)
                    can_use = True
                    break

            if not can_use:
                raise RuntimeError("cache file [%s] is using, operate file error" % self.filename)

            file_name = self.path + self.filename
            cache_dict = {}

            try:
                # 不清除缓存时，读缓存
                if os.path.exists(file_name):
                    with open(file_name, "r") as f:
                        cache_dict = pickle.load(f)

                cache_params_str = cache_dict.get("param_str", "")
                param_str = self._gen_param_str(*args, **kwargs)

                # 函数参数相同并且在有效期内，缓存可以使用。否则使用函数，缓存结果
                if cache_params_str == param_str and time.time() - int(cache_dict["time_stamp"]) < self.expire:
                    cache_data = cache_dict["data"]
                    result = cache_data

                else:
                    result = func(*args, **kwargs)

                    # 写缓存
                    with open(file_name, "w") as f:
                        cache_dict = {
                            "key": self.key,
                            "param_str": param_str,
                            "time_stamp": time.time(),
                            "data": result
                        }
                        pickle.dump(cache_dict, f)
            except Exception:
                six.reraise(*sys.exc_info())
            finally:
                self.FILE_NAME_LOCK_LIST.remove(self.filename)

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
        code.update(str(self.key))
        code.update("".join(sorted([str(i) for i in args])))
        code.update("".join(sorted([str(i) for i in kwargs.iteritems()])))
        return code.hexdigest()

    def clear_cache(self):
        """
        清除cache
        :return: None
        """
        file_name = self.path + self.filename
        self.FILE_NAME_LOCK_LIST.append(self.filename)

        with open(file_name, "w") as file:
            file.write("{}")

        self.FILE_NAME_LOCK_LIST.remove(self.filename)


if __name__ == "__main__":
    @FileCache("add")
    def add(a, b):
        print "no cache"
        return a + b


    c = add(1, 2)
