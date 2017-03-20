#!/usr/bin/env python

# @author Javon Negahban
# Tiny connection library for pymysql to ensure database connections are alive.

import pymysql

__all__ = ['getConn']

def connect():
  return pymysql.connect(host='localhost', user='root', passwd='', db='OverlordDB', cursorclass=pymysql.cursors.DictCursor)

def getConn():
  global conn
  try:
    conn.ping(True)
  except:
    conn = connect()
  return conn

conn = connect()

