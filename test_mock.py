#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado import gen
import tornado.ioloop
import tornado.testing
import tornado.concurrent
import tornado.httpserver
import tornado.web
from tornado.options import define, options
from tornado.ioloop import PeriodicCallback
import tornado.autoreload
import unittest
import mock

define('port', default=8423, help='run on this port', type=int)
tornado.options.parse_command_line()


class Application(tornado.web.Application):
    """
    应用类
    """

    def __init__(self):
        handlers = [
            (r'/', IndexHandler),
        ]
        super(Application, self).__init__(handlers)


class IndexHandler(tornado.web.RequestHandler):
    """
    首页
    """

    def get(self):
        self.write("hello")


class Service(object):
    @gen.coroutine
    def test(self):
        res = yield MetaService().meta_test()
        res1 = yield MetaService1().test1()
        res2 = MetaService1().test_hello()
        raise gen.Return(res + res1 + res2)


class MetaService(object):
    def meta_test(self):
        gen.Return("hello world")


class Meta(object):
    def hello(self):
        return "meta hello"


class MetaService1(Meta):
    @gen.coroutine
    def test1(self):
        raise gen.Return(" hello world 1")

    def test_hello(self):
        return self.hello()


class TestIDCardAuth(tornado.testing.AsyncHTTPSTestCase):
    """
    # 测试实名认证
    """

    def setUp(self):
        super(TestIDCardAuth, self).setUp()

    def get_app(self):
        return Application()

    @mock.patch.object(MetaService1, "test1")
    @mock.patch.object(MetaService, "meta_test")
    @mock.patch.object(Meta, "hello")
    @tornado.testing.gen_test
    def test_inst(self, mock_test1, mock_meta_test, mock_hello):
        future = tornado.concurrent.Future()
        future.set_result("no hello")
        mock_test1.return_value = future

        future1 = tornado.concurrent.Future()
        future1.set_result(" 你好！")
        mock_meta_test.return_value = future1

        mock_hello.return_value = "hello"
        res = yield Service().test()


        print res


if __name__ == "__main__":
    suit = unittest.TestSuite()

    test_list = [TestIDCardAuth("test_inst")]

    suit.addTests(test_list)

    runner = unittest.TextTestRunner(verbosity=1)
    runner.run(suit)
#
# if __name__ == "__main__":
#     application = Application()
#     http_server = tornado.httpserver.HTTPServer(application)
#     http_server.listen(8080)
#     tornado.ioloop.IOLoop.current().start()
