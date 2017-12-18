#!/usr/bin/env python
# -*- coding:utf-8 -*-
class SingletonMeta(type):
    def __init__(cls, name, bases, dictionary):
        super(SingletonMeta, cls).__init__(name, bases, dictionary)
        cls._instance = None

    def __call__(self, *args, **kw):
        if self._instance is None:
            self._instance = super(SingletonMeta, self).__call__(*args, **kw)

        return self._instance


class Singleton(object):
    """
    单例类
    继承此类后，类都为Singleton
    """
    __metaclass__ = SingletonMeta
#
#
# class A(Singleton):
#     """docstring for A"""
#
#     def __init__(self, a):
#         super(A, self).__init__()
#         self.a = a
#
#
# class B(A):
#     """docstring for B"""
#
#     def __init__(self, a, b):
#         super(B, self).__init__(a)
#         self.b = b
#
#
# class C(B, dict):
#     """docstring for C"""
#
#     def __init__(self, a, b, c):
#         super(C, self).__init__(a, b)
#         super(A, self).__init__([(a, a), (b, b)])
#         self.c = c
#
#
# a = A(1)
# a1 = A(2)
# print a.a
# print a1.a
# print id(a) == id(a1)
# print "*" * 8
#
# b = B(10, 2)
# b1 = B(3, 4)
# print b.a
# print b1.a
# print id(b) == id(b1)
# print "*" * 8
#
# c = C(100, 2, 3)
# c1 = C(4, 5, 6)
# print c.a
# print c1.a
# print id(c) == id(c1)
# print "*" * 8
# c.update({3: 3})
# print c
