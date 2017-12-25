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


class EtcdWatchBase(Singleton):
    """
    etcd 异步获得key的更新
    """
    Lock_Item = namedtuple("Lock_Item", ("future", "token", "ttl"))
    UNLOCK = "no_lock"

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param str key: 监控的关键字
        :param tuple etcd_servers: etcd 服务器列表
        """
        self.watch_root = watch_root
        self.etcd_servers = etcd_servers
        self.sentry = sentry
        self.watch_dict = defaultdict(list)
        self.wait_lock_dict = defaultdict(list)
        self.locked_dict = dict()
        self.thread = None
        self.client = etcd.Client(host=self.etcd_servers, allow_reconnect=True)

    def run(self):
        """
        循环阻塞获得etcd key的变化
        """
        while True:
            try:
                res = self.client.get(self.watch_root)
                self.process_etcd(res)
                for res in self.client.eternal_watch(self.watch_root, recursive=True):
                    logging.error("获得新的 etcd")
                    self.process_etcd(res)

            except Exception as exp:
                logging.error("thread error: %s", str(exp))
                if self.sentry:
                    self.sentry.captureException(exc_info=True)

    def process_etcd(self, res):
        etcd_key = res.key
        etcd_value = res.value

        # watch key
        if etcd_key in self.watch_dict.keys():
            future_list = self.watch_dict[etcd_key]

            # 唤醒所有等待的watch
            for future in future_list:
                future.set_result((True, etcd_value))
            del self.watch_dict[etcd_key]

        # lock key
        elif etcd_key in self.wait_lock_dict:
            wait_lock_list = self.wait_lock_dict[etcd_key]

            # 解锁状态 或者 锁超时 进行解锁操作
            if etcd_value in (self.UNLOCK, None) and wait_lock_list:
                lock_item = wait_lock_list[0]
                logging.error("watch lock")
                lock_flag = self._lock(etcd_key, lock_item.token, lock_item.ttl)
                if lock_flag:
                    lock_item.future.set_result((True, lock_item.token))
                    self.locked_dict[etcd_key] = lock_item
                    self.wait_lock_dict[etcd_key].pop(0)

            # 锁超时
            elif not etcd_value:
                if self.locked_dict.get(etcd_key):
                    ttl = self.locked_dict[etcd_key].ttl
                    del self.locked_dict[etcd_key]
                    raise RuntimeError("key: %s ttl: %s，锁超时" % (etcd_key, ttl))

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

    def _lock(self, key, token, ttl):
        """

        :param str key: 关键字
        :param str lock_uuid: lock码
        :return: bool
        """
        try:
            self.client.test_and_set(key, token, self.UNLOCK, ttl)
            logging.error("def _lock 加锁")
            return True

        except etcd.EtcdKeyNotFound:
            try:
                self.client.write(key, token, prevExist=False, recursive=True, ttl=ttl)
                logging.debug("write")
                return True
            except etcd.EtcdAlreadyExist as e:
                logging.debug("had lock %s", e)
                return False

        except etcd.EtcdCompareFailed as e:
            logging.error("EtcdCompareFailed: %s 加锁失败", e)
            return False

    def watch(self, key):
        """
        监控key
        :param str key: 关键字
        :return:
        """
        future = Future()
        # 参数检查
        if not key.startswith(self.watch_root) or key in self.wait_lock_dict or key in self.locked_dict:
            future.set_result((False, None))
            return future

        self._start_thread()

        self.watch_dict[key].append(future)
        return future

    def lock(self, key, ttl=None):
        """
        加锁
        :param str key: 关键字
        :param int ttl: 过期时间
        :return: Future object
        """
        future = Future()
        # 参数检查
        if not key.startswith(self.watch_root) or key in self.watch_dict:
            future.set_result((False, None))
            return future

        self._start_thread()

        token = str(uuid.uuid4())
        lock_item = self.Lock_Item(future, token, ttl)
        lock_flag = self._lock(key, token, ttl)
        if lock_flag:
            self.locked_dict[key] = lock_item
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
            self.client.test_and_set(key, self.UNLOCK, token)
            logging.error("解锁 %s", token)
            return True
        except (etcd.EtcdCompareFailed, etcd.EtcdKeyNotFound) as e:
            logging.error("在未加锁的情况下，进行解锁操作")
            logging.error(e)
            return False

    def start_loop(self, function_list):
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


if __name__ == "__main__":
    ETCD_SERVERS = (("127.0.0.1", 2379),)
    etcd_watch = EtcdWatchBase("/watch", ETCD_SERVERS, None)


    @gen.coroutine
    def test():

        # res = yield etcd_watch.get("/watch/a")

        # res = yield etcd_watch.watch("/watch/aa")

        # res = yield etcd_watch.get("/watch/aaa")

        # a = "hello"
        # a += "q"
        import time
        start_time = time.time()
        flag, token = yield etcd_watch.lock("/watch/lock/aa1")
        logging.error(flag, token)

        def tmp():
            f = Future()
            IOLoop.current().call_later(1, lambda: f.set_result(None))
            return f

        yield tmp()
        res = etcd_watch.unlock("/watch/lock/aa1", token)
        logging.error("执行时间：")
        logging.error(time.time() - start_time)
        raise gen.Return(res)


    etcd_watch.start_loop([test, test, test, test, test, test, test])

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
