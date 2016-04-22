#!/usr/bin/env python

from pytd import result
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

def test_mysql_default():
    subject = result.MySQLResultOutput()
    assert subject.hostname == None
    assert subject.port == 3306
    assert subject.username == None
    assert subject.password == None
    assert subject.database == None
    assert subject.table == None
    assert subject.ssl == False
    assert subject.ssl_verify == False
    assert subject.mode == 'append'
    assert subject.unique == None
    assert subject.useCompression == True

def test_mysql_ctor():
    subject = result.MySQLResultOutput(hostname = 'hostname', port = 13306, username = 'username', password = 'password', database = 'database', table = 'table', ssl = True, ssl_verify = False)
    assert subject.hostname == 'hostname'
    assert subject.port == 13306
    assert subject.username == 'username'
    assert subject.password == 'password'
    assert subject.database == 'database'
    assert subject.table == 'table'
    assert subject.ssl == True
    assert subject.ssl_verify == False
    assert subject.mode == 'append'
    assert subject.unique == None
    assert subject.useCompression == True
    o = urlparse.urlparse(subject.get_result_url())
    assert o.hostname == 'hostname'
    assert o.port == 13306
    assert o.username == 'username'
    assert o.password == 'password'
    assert o.path == '/database/table'
    query = urlparse.parse_qs(o.query)
    assert query['ssl'] == ['true']
    assert query['ssl_verify'] == ['false']
    assert query['mode'] == ['append']
    assert query['useCompression'] == ['true']
