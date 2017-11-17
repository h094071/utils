#!/usr/bin/env python
# -*- coding: utf-8 -*-
import fcntl
import os
import sys
import multiprocessing
import time


class Lock(object):
    """文件锁"""

    def __init__(self, name):
        """
        :param name: 文件名
        """
        self.fobj = open(name, 'w')
        self.fd = self.fobj.fileno()

    def lock(self):
        try:
            fcntl.lockf(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # 给文件加锁，使用了fcntl.LOCK_NB
            print '给文件加锁，稍等'
            time.sleep(1)
            return True
        except Exception as e:
            print '文件加锁，无法执行，请稍后运行。', e
            return False

    def unlock(self):
        self.fobj.close()
        print '已解锁'

    def __call__(self, *args, **kwargs):
        while not self.lock():
            self.fobj.write("hello")
        self.unlock()

if __name__ == "__main__":
    for i in range(5):
        p = multiprocessing.Process(target=Lock("ss.txt"))
        p.start()
        p.join()