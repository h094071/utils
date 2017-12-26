#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
etcd 异步 服务发现 异步 锁
"""
import os
import sys
import uuid
from collections import namedtuple, defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
import etcd
import threading
import logging
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.concurrent import Future
from singleton import Singleton


class EtcdProcess(object):
    """"
    etcd 处理基类
    """

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param str key: 监控的关键字
        :param tuple etcd_servers: etcd 服务器列表
        """
        self.watch_root = watch_root
        self.client = etcd.Client(host=etcd_servers, allow_reconnect=True)
        self.sentry = sentry
        self._local_index = None
        self.key_process_dict = dict()
        self.thread = None

    def run(self):
        """
        循环阻塞获得etcd key的变化
        """
        while True:
            try:
                response = self.client.watch(self.watch_root, recursive=True, timeout=0, index=self._local_index)
                self._local_index = response.modifiedIndex + 1

                etcd_key = response.key

                dir_list = etcd_key.split("/")

                process_dir = dir_list[2]

                if self.key_process_dict.get(process_dir):
                    response.dir_list = dir_list[3:]
                    self.key_process_dict[process_dir](response)

            except Exception as exp:
                logging.error("thread error: %s", str(exp))
                if self.sentry:
                    self.sentry.captureException(exc_info=True)

    def _start_thread(self):
        """
        开启线程并确保线程可用
        :return:
        """
        if not self.thread or not self.thread.is_alive():
            t = threading.Thread(target=self.run)
            self.thread = t
            t.setDaemon(True)
            t.start()
            logging.error("start thread")

    def register(self, key, process):
        """

        :param key:
        :param process:
        :return:
        """
        if key in self.key_process_dict:
            raise RuntimeError("key: %s has existed" % key)
        self.key_process_dict[key] = process


class ProcessBase(Singleton):
    """

    """

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param key:
        :param etcd_servers:
        :param sentry:
        """
        self.watch_root = watch_root
        self.etcd_process = EtcdProcess(watch_root, etcd_servers, sentry)
        self.client = etcd.Client(host=etcd_servers, allow_reconnect=True)
        self.watch_dict = defaultdict(list)
        self.etcd_process.register(self.base_key, self.process)

    def get_key(self, key):
        """

        :param key:
        :return:
        """
        dir_list = self.watch_root.split("/")
        dir_list.extend([self.base_key, key])
        return "/".join(dir_list)

    def test_and_set(self, key, value, prev_value, ttl=None):
        """

        :param key:
        :param value:
        :param prev_value:
        :param ttl:
        :return:
        """
        key = self.get_key(key)
        return self.client.test_and_set(key, value, prev_value, ttl)

    def set(self, key, value, ttl=None, dir=False, append=False, **kwdargs):
        key = self.get_key(key)
        return self.client.write(key, value, ttl, dir, append, **kwdargs)

    def process(self, etcd_value):
        """

        :param etcd_value:
        :return:
        """
        raise NotImplementedError


class WatchProcess(ProcessBase):
    """
    监控关键字，
    """

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param key: 
        :param etcd_servers: 
        :param sentry: 
        """
        self.base_key = "watch"
        super(WatchProcess, self).__init__(watch_root, etcd_servers, sentry)

    def watch_key(self, key):
        """

        :param key:
        :return:
        """
        self.etcd_process._start_thread()

        future = Future()
        self.watch_dict[key].append(future)
        return future

    def process(self, etcd_res):
        """

        :param etcd_res:
        :return:
        """
        etcd_key = etcd_res.dir_list[0]
        etcd_value = etcd_res.value

        if etcd_key in self.watch_dict.keys():
            future_list = self.watch_dict[etcd_key]
            # 唤醒所有等待的watch
            for future in future_list:
                future.set_result((True, etcd_value))

            del self.watch_dict[etcd_key]


class LockProcess(ProcessBase):
    """
    锁
    """
    UNLOCK = "no_lock"
    Lock_Item = namedtuple("Lock_Item", ("future", "token", "ttl"))

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param key:
        :param etcd_servers:
        :param sentry:
        """
        self.base_key = "watch"
        self.wait_lock_dict = defaultdict(list)
        self.locking_dict = dict()
        super(LockProcess, self).__init__(watch_root, etcd_servers, sentry)

    def process(self, etcd_res):
        """

        :param etcd_res:
        :return:
        """
        etcd_key = etcd_res.dir_list[0]
        etcd_value = etcd_res.value

        wait_lock_list = self.wait_lock_dict[etcd_key]

        # 解锁状态 或者 锁超时 进行解锁操作
        if etcd_value in (self.UNLOCK, None) and wait_lock_list:
            lock_item = wait_lock_list[0]
            logging.error("watch lock")
            lock_flag = self._lock(etcd_key, lock_item.token, lock_item.ttl)
            if lock_flag:
                lock_item.future.set_result((True, lock_item.token))
                self.locking_dict[etcd_key] = lock_item
                self.wait_lock_dict[etcd_key].pop(0)

        # 锁超时
        elif not etcd_value:
            if self.locking_dict.get(etcd_key):
                ttl = self.locking_dict[etcd_key].ttl
                del self.locking_dict[etcd_key]
                raise RuntimeError("key: %s ttl: %s，锁超时" % (etcd_key, ttl))

    def _lock(self, key, token, ttl):
        """

        :param str key: 关键字
        :param str lock_uuid: lock码
        :return: bool
        """
        try:

            self.test_and_set(key, token, self.UNLOCK, ttl)
            return True

        except etcd.EtcdKeyNotFound:
            try:
                self.set(key, token, prevExist=False, recursive=True, ttl=ttl)
                return True
            except etcd.EtcdAlreadyExist as e:
                logging.debug(e)
                return False

        except etcd.EtcdCompareFailed as e:
            logging.debug(e)
            return False

    def lock(self, key, ttl=None):
        """
        加锁
        :param str key: 关键字
        :param int ttl: 过期时间
        :return: Future object
        """
        self.etcd_process._start_thread()
        future = Future()

        token = uuid.uuid4().hex
        lock_item = self.Lock_Item(future, token, ttl)
        lock_flag = self._lock(key, token, ttl)
        if lock_flag:
            self.locking_dict[key] = lock_item
            future.set_result((True, token))
        else:
            self.wait_lock_dict[key].append(lock_item)
        return future

    def unlock(self, key, token):
        """
        解锁
        :param key:
        :return:
        """
        try:
            self.test_and_set(key, self.UNLOCK, token)
            logging.error("解锁 %s", token)
            return True
        except (etcd.EtcdCompareFailed, etcd.EtcdKeyNotFound) as e:
            logging.error("在未加锁的情况下，进行解锁操作")
            logging.error(e)
            return False


if __name__ == "__main__":
    ETCD_SERVERS = (("127.0.0.1", 2379),)
    etcd_watch = LockProcess("/watch", ETCD_SERVERS, None)


    def start_loop(function_list):
        """
        仅供脚本使用
        :param function_list:
        :return:
        """
        io_loop = IOLoop.instance()
        future_list = []

        def stop(future):
            future_list.remove(future)
            if not future_list:
                io_loop.stop()

        for function in function_list:
            future = function()
            future_list.append(future)
            io_loop.add_future(future, lambda x: stop(x))

        io_loop.start()

    @gen.coroutine
    def test():
        # res = yield etcd_watch.get("/watch/a")

        # res = yield etcd_watch.watch("/watch/aa")

        # res = yield etcd_watch.get("/watch/aaa")

        # a = "hello"
        # a += "q"
        import time
        start_time = time.time()
        flag, token = yield etcd_watch.lock("ba")
        logging.error(token)

        def tmp():
            f = Future()
            IOLoop.current().call_later(1, lambda: f.set_result(None))
            return f

        yield tmp()
        res = etcd_watch.unlock("ba", token)
        logging.error("执行时间：")
        logging.error(time.time() - start_time)
        raise gen.Return(res)

    start_loop([test, test, test, test, test, test, test, test, test, test, test, test, ])
    # IOLoop.current().run_sync(test)
    # def main():
    #     IOLoop.current().run_sync(test)
    #
    #
    # from multiprocessing import Process
    #
    # for _ in range(2):
    #     p = Process(target=main)
    #     p.daemon = False
    #     p.start()
