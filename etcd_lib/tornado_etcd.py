#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
etcd 异步 服务发现 异步 锁
"""
import uuid
from collections import namedtuple, defaultdict
import etcd
import threading
import logging
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.concurrent import Future
from singleton import Singleton


class EtcdProcess(object):
    """"
    etcd 线程
    """

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param str watch_root: 监控的根目录
        :param tuple etcd_servers: etcd 服务器列表
        :param dict sentry: sentry object
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
                logging.error("tornado etcd thread error: %s", str(exp))
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
            logging.info("tornado etcd start thread")

    def register(self, key, process):
        """
        注册处理器 eg: key="lock" process=lock_process
        :param str key: 关注的目录
        :param func process: 处理器
        :return:
        """
        if key in self.key_process_dict:
            raise RuntimeError("tornado etcd key: %s has existed" % key)
        self.key_process_dict[key] = process


class ProcessBase(Singleton):
    """
    处理器基类
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

    def get_full_key(self, key):
        """
        获得带路径的key值 eg: key = "read_lock" return /base_dir/lock/read_lock
        :param str key: 部分key值
        :return: "获得带路径的key值"
        """
        dir_list = self.watch_root.split("/")
        dir_list.extend([self.base_key, key])
        return "/".join(dir_list)

    def test_and_set(self, key, value, prev_value, ttl=None):
        """
        etcd test_and_set 封装
        :param str key: 部分key值
        :param value: 设置的值
        :param prev_value: 设置前的值
        :param ttl: 过期时间
        :return:
        """
        key = self.get_full_key(key)
        return self.client.test_and_set(key, value, prev_value, ttl)

    def set(self, key, value, ttl=None, dir=False, append=False, **kwdargs):
        """
        etcd set 封装
        :param key: 部分key值
        :param value: 设置的值
        :param ttl: 过期时间
        :param dir: 目录
        :param append: 
        :param kwdargs: 
        :return: 
        """
        key = self.get_full_key(key)
        return self.client.write(key, value, ttl, dir, append, **kwdargs)

    def get(self, key, **kwdargs):
        """
        etcd get 封装
        :param key: 
        :param kwdargs: 
        :return: 
        """
        key = self.get_full_key(key)
        return self.client.read(key, **kwdargs)

    def process(self, etcd_value):
        """
        处理器
        :param etcd_value:
        :return:
        """
        raise NotImplementedError


class WatchProcess(ProcessBase):
    """
    监控关键字，关键字不变化，协程阻塞，关键字有变化，协程继续执行
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
        监控关键字，关键字不变化，协程阻塞，关键字有变化，协程继续执行
        :param str key: 监控的关键字
        :return:
        """
        self.etcd_process._start_thread()

        future = Future()
        self.watch_dict[key].append(future)
        return future

    def process(self, etcd_res):
        """
        监控关键字处理器
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


class EternalWatchProcess(ProcessBase):
    """
    监控关键字，关键字不变化，协程阻塞，关键字有变化，协程继续执行
    """

    def __init__(self, watch_root, etcd_servers, sentry=None):
        """
        init
        :param key:
        :param etcd_servers:
        :param sentry:
        """
        self.base_key = "eternal_watch"
        super(EternalWatchProcess, self).__init__(watch_root, etcd_servers, sentry)

    def get_key(self, key):
        """
        监控关键字，关键字不变化，协程阻塞，关键字有变化，协程继续执行
        :param str key: 监控的关键字
        :return:
        """
        self.etcd_process._start_thread()

        if not self.watch_dict.get(key):
            # etcd 中必须提前设置该key，否则报错
            etcd_res = self.get(key)
            self.watch_dict[key] = etcd_res.value

        return self.watch_dict[key]

    def process(self, etcd_res):
        """
        监控关键字处理器
        :param etcd_res:
        :return:
        """
        etcd_key = etcd_res.dir_list[0]
        etcd_value = etcd_res.value

        if etcd_key in self.watch_dict.keys():
            # 更新值
            self.watch_dict[etcd_key] = etcd_value


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
        self.base_key = "lock"
        self.wait_lock_dict = defaultdict(list)
        self.locking_dict = dict()
        super(LockProcess, self).__init__(watch_root, etcd_servers, sentry)

    def process(self, etcd_res):
        """
        锁处理器
        :param etcd_res:
        :return:
        """
        etcd_key = etcd_res.dir_list[0]
        etcd_value = etcd_res.value

        wait_lock_list = self.wait_lock_dict[etcd_key]

        # 解锁状态 或者 锁超时 进行解锁操作
        if etcd_value in (self.UNLOCK, None) and wait_lock_list:
            lock_item = wait_lock_list[0]
            lock_flag = self._lock(etcd_key, lock_item)

            if lock_flag:
                logging.info("tornado etcd lock sucess key:%s", etcd_key)
                lock_item.future.set_result((True, lock_item.token))
                self.locking_dict[etcd_key] = lock_item
                self.wait_lock_dict[etcd_key].pop(0)

        # 锁超时
        elif not etcd_value:
            if self.locking_dict.get(etcd_key):
                ttl = self.locking_dict[etcd_key].ttl
                del self.locking_dict[etcd_key]
                raise RuntimeError("key: %s ttl: %s，lock time out" % (etcd_key, ttl))

    def _lock(self, key, lock_item):
        """
        尝试加锁
        :param str key: 关键字
        :param str lock_uuid: lock码
        :return: bool 加锁是否成功
        """
        token = lock_item.token
        ttl = lock_item.ttl
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
        lock_flag = self._lock(key, lock_item)
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
            logging.info("tornado etcd unlock key: %s, token: %s", key, token)
            return True
        except (etcd.EtcdCompareFailed, etcd.EtcdKeyNotFound) as e:
            logging.error("tornado etcd unlock before lock %s", e)
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
