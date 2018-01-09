#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mock
def aa(a,b,c):
    pass


def mock_func(*args, **kwargs):
    kk = range(10)
    return kk[args[0]]

mock_aa = mock.create_autospec(aa, return_value="hello", side_effect=mock_func)
kkkk = mock_aa(5,2,3)
print kkkk
