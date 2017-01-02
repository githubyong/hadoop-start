#!/usr/bin/env python
# -*- coding: utf-8 -*
__author__ = 'yong'  # on 2017/1/2
# 生成3个文件，每个文件随机写3000个随机数

import random

arr = []
for i in range(0, 3000):
    arr.insert(random.randint(0, len(arr)), i)

for i in arr:
    print i

for i in range(0, 3):
    try:
        with open('input' + str(i) + '.txt', 'w+') as file:
            for j in range(0, 1000):
                n = arr[1000 * i + j] + 10000
                s = str(n) + " str" + str(n) + "\n";
                file.write(s)
            file.close()
    except IOError as e:
        print(e)
