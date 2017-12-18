#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
常量定义
"""
import time
from tornado import gen
import tornado.web
import tornado.gen
from tornado.ioloop import IOLoop
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
class HasBlockTaskHandler():
    executor = ThreadPoolExecutor(4) #起线程池，由当前RequestHandler持有

    @run_on_executor
    def block_task(self):
        time.sleep(5000) #也可能是其他耗时／阻塞型任务
        return 123 #直接return结果即可


@tornado.gen.coroutine
def get():
    ss = HasBlockTaskHandler()
    result = yield ss.block_task() #block_task将提交给线程池运行
    raise gen.Return(result)

IOLoop.current().run_sync(get)