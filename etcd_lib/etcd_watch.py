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

UNLOCK = "no_lock"


class EtcdWatch(Singleton):
    """
    etcd 异步获得key的更新
    """
    Lock_Item = namedtuple("Lock_Item", ("future", "token", "ttl"))

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
                for res in self.client.eternal_watch(self.watch_root, recursive=True):
                    logging.error("获得新的 etcd %s", res)
                    etcd_key = res.key
                    etcd_value = res.value

                    # watch key
                    if etcd_key in self.watch_dict.keys():
                        future_list = self.watch_dict[etcd_key]

                        # 唤醒所有等待的watch
                        for future in future_list:
                            future.set_result(etcd_value)
                        del self.watch_dict[etcd_key]

                    # lock key
                    elif etcd_key in self.wait_lock_dict:
                        wait_lock_list = self.wait_lock_dict[etcd_key]

                        # 解锁状态 或者 锁超时 进行解锁操作
                        if etcd_value in (UNLOCK, None) and len(wait_lock_list):
                            lock_item = wait_lock_list[0]
                            lock_flag = self._lock(etcd_key, lock_item.token, lock_item.ttl)
                            if lock_flag:
                                lock_item.future.set_result(lock_item.token)
                                self.locked_dict[etcd_key] = lock_item
                                self.wait_lock_dict[etcd_key].pop(0)

                        # 锁超时
                        elif not etcd_value:
                            if self.locked_dict.get(etcd_key):
                                ttl = self.locked_dict[etcd_key].ttl
                                del self.locked_dict[etcd_key]
                                raise RuntimeError("key: %s ttl: %s，锁超时" % (etcd_key, ttl))
                            else:
                                raise RuntimeError("key: %s 锁异常" % (etcd_key))


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

    def _lock(self, key, token, ttl):
        """

        :param str key: 关键字
        :param str lock_uuid: lock码
        :return: bool
        """
        try:
            self.client.test_and_set(key, token, UNLOCK, ttl)
            logging.error("def _lock 加锁")
            return True

        except etcd.EtcdKeyNotFound:
            try:
                self.client.write(key, token, prevExist=False, ttl=ttl)
                return True
            except etcd.EtcdAlreadyExist:
                logging.debug("had lock")
                return False

        except etcd.EtcdCompareFailed:
            logging.error("EtcdCompareFailed 加锁失败")
            return False

    def watch(self, key):
        """
        监控key
        :param str key: 关键字
        :return:
        """
        self._start_thread()

        future = Future()
        self.watch_dict[key].append(future)
        return future

    def lock(self, key, ttl=0):
        """
        加锁
        :param str key: 关键字
        :param int ttl: 过期时间
        :return: Future object
        """
        self._start_thread()

        future = Future()
        token = str(uuid.uuid4())
        lock_item = self.Lock_Item(future, token, ttl)
        lock_flag = self._lock(key, token, ttl)
        if lock_flag:
            self.locked_dict[key] = lock_item
            future.set_result(token)
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
            logging.error("解锁 start")
            res = self.client.test_and_set(key, UNLOCK, token)

            logging.error(res)
            logging.error("解锁 %s", token)
            return True
        except etcd.EtcdCompareFailed, etcd.EtcdKeyNotFound:
            logging.error("在未加锁的情况下，进行解锁操作")
            logging.error("unlock")
            return False


if __name__ == "__main__":
    ETCD_SERVERS = (("127.0.0.1", 2379),)
    etcd_watch = EtcdWatch("/watch", ETCD_SERVERS, None)


    @gen.coroutine
    def test():
        # print "start"
        # res = yield etcd_watch.get("/watch/a")
        # print res
        # res = yield etcd_watch.watch("/watch/aa")
        # print res
        # res = yield etcd_watch.get("/watch/aaa")
        # print res
        # print "======"
        # a = "hello"
        # a += "q"
        token = yield etcd_watch.lock("/watch/lock/a")
        print token
        print "111222222"
        import time
        time.sleep(5)
        res = etcd_watch.unlock("/watch/lock/a", token)
        res = "success"
        raise gen.Return(res)


    IOLoop.current().run_sync(test)
