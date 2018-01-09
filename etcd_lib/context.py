#!/usr/bin/env python
# -*- coding: utf-8 -*-
from etcd_lib.tornado_etcd import EternalWatchProcess


class MClient(object):
    def get(self, key):
        """
        get操作
        :param key: 键值
        """
        ETCD_SERVERS = (("127.0.0.1", 2379),)
        etcd_srv = EternalWatchProcess("/watch", ETCD_SERVERS, None)
        key_list = etcd_srv.get_full_key("memcash_toggle_key")
        for i in key_list:
            if key.startswith(i):
                return None

        return "memcash result"





