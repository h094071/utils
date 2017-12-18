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
import logging

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

        logging.error("hello")
        http_client = AsyncHTTPClient()
        req = HTTPRequest("https://www.baidu.com/")
        res = yield http_client.fetch(req)
        self.write('1111111')

if __name__ == "__main__":
    application = Application()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8080)
    tornado.ioloop.IOLoop.current().start()
