# -*- coding: utf-8 -*-
import sqlite3
conn = sqlite3.connect('data/stock_kline.db')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM kline')
print('Total klines:', cur.fetchone()[0])
cur.execute('SELECT COUNT(DISTINCT symbol) FROM kline')
print('Total stocks:', cur.fetchone()[0])
conn.close()
