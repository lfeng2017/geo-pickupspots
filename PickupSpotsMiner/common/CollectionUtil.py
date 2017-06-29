# -*- coding: utf-8 -*-

def chunks(lst, n):
    '''
    将list分段，供mapPartition使用

    :param lst:
    :param n:
    :return:
    '''

    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def sublists(lst, n):
    '''
    将list按其总长度平均分为 n 份

    :param lst: 待分割的list
    :param n: 分割的份数
    :return: 子list集合， e.g. [[1,2,3], [4,5,6], [7,8,9]]
    '''

    return [lst[i:i + n] for i in range(0, len(lst), n)]
