#!/usr/bin/env python
from tornado import gen
from tornado.ioloop import IOLoop

@gen.coroutine
def main():
  yield gen.sleep(1000)

  print 'aaa'
  gen.Return("hello")

IOLoop.current().run_sync(main)
