#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
常量定义
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
import etcd
import threading
import logging
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.concurrent import Future
from singleton import Singleton

ETCD_SERVERS = (("127.0.0.1", 2379),)

LOCK = "1"
UNLOCK = "0"

LOCK_FAIL = -1

LOCK_SUCC = 1
WAIT_LOCK = 2


class EtcdWatch(Singleton):
    """
    etcd 异步获得key的更新
    """

    def __init__(self, watch_dir, etcd_servers, sentry=None):
        """
        init
        :param str key: 监控的关键字
        :param tuple etcd_servers: etcd 服务器列表
        """
        self.watch_dir = watch_dir
        self.etcd_servers = etcd_servers
        self.sentry = sentry
        self.key_dict = dict()
        self.thread = None
        self.client = etcd.Client(host=self.etcd_servers, allow_reconnect=True)

    def run(self):
        """
        循环阻塞获得etcd key的变化
        """
        while True:
            try:
                for res in self.client.eternal_watch(self.watch_dir, recursive=True):
                    logging.error("获得新的 etcd %s", res)
                    etcd_key = res.key
                    if etcd_key in self.key_dict.keys():
                        future, is_lock = self.key_dict[etcd_key]

                        if is_lock:
                            # 处于解锁状态
                            if res.value != LOCK:
                                logging.error(res.value != LOCK)
                                logging.error(res.value)
                                lock_flag = self._lock(etcd_key, res.modifiedIndex)
                                if not lock_flag:
                                    continue
                            else:
                                # 处于加锁状态
                                continue

                        future.set_result(res.value)
                        del self.key_dict[etcd_key]

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

    def _lock(self, key, etcd_index):
        """

        :param str key: 关键字
        :param int etcd_index: 索引
        :return: bool
        """
        try:
            logging.error("加锁 start1")
            set_res = self.client.test_and_set(key, LOCK, UNLOCK)
            logging.error("加锁")
        except etcd.EtcdCompareFailed:
            logging.error("加锁失败")
            return False
        except Exception:
            logging.error("加锁失败")
            if self.sentry:
                self.sentry.captureException(exc_info=True)
            return False

        if set_res._prev_node.modifiedIndex != etcd_index:

            logging.error("加锁失败, 索引不同")
            logging.error(set_res)
            return False
        else:
            return True

    def _get_key(self, key, is_lock=False):
        """
        异步获得key的值
        :param str key: 关键字
        :param bool is_lock: 是否是锁
        :return: Future object
        """
        self._start_thread()
        if key not in self.key_dict.keys():
            future = Future()
            self.key_dict[key] = (future, is_lock)

        return self.key_dict[key][0]

    def watch(self, key):
        """
        监控关键字变化
        :param str key: 关键字
        :return: Future object
        """
        return self._get_key(key)

    def lock(self, key):
        """
        加锁
        :param str key: 关键字
        :return: Future object
        """
        lock_status = LOCK_SUCC
        try:
            read_res = self.client.read(key)
            logging.error(read_res)
        except etcd.EtcdKeyNotFound:
            logging.error("初始化，加锁 start2")
            self.client.set(key, LOCK)
            logging.error("初始化，加锁")
            lock_status = LOCK_FAIL
        except Exception:
            logging.error("初始化锁失败")
            if self.sentry:
                self.sentry.captureException(exc_info=True)
            lock_status = LOCK_FAIL
        else:
            if read_res.value != LOCK:
                lock_status = self._lock(key, read_res.etcd_index)

        future = Future()
        if lock_status == LOCK_SUCC:
            future.set_result(LOCK)
            return future
        elif lock_status == LOCK_FAIL:
            future.set_result(LOCK_FAIL)
            return future

        return self._get_key(key, is_lock=True)

    def unlock(self, key):
        """
        解锁
        :param key:
        :return:
        """
        try:
            logging.error("解锁 start")
            self.client.test_and_set(key, UNLOCK, LOCK)
            logging.error("解锁")

        except etcd.EtcdCompareFailed:
            logging.error("在未加锁的情况下，进行解锁操作")
            logging.error("unlock")
            return False
        except Exception:
            logging.error("解锁失败")
            if self.sentry:
                self.sentry.captureException(exc_info=True)
            return False
        else:
            return True


etcd_watch = EtcdWatch("/watch", ETCD_SERVERS, None)


@gen.coroutine
def test():
    # print "start"
    # res = yield etcd_watch.get("/watch/a")
    # print res
    # res = yield etcd_watch.get("/watch/aa")
    # print res
    # res = yield etcd_watch.get("/watch/aaa")
    # print res
    # print "======"
    # a = "hello"
    # a += "q"
    res = yield etcd_watch.lock("/watch/lock/a")
    print res
    print "111222222"
    import time
    time.sleep(10)
    raise ValueError("test")
    res = etcd_watch.unlock("/watch/lock/a")
    res = "success"
    raise gen.Return(res)


IOLoop.current().run_sync(test)
