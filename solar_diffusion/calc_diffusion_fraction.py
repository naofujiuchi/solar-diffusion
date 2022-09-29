# -*- coding: utf-8 -*-
# Naomichi Fujiuchi (naofujiuchi@gmail.com), September 2022
# This is an original work by Fujiuchi (MIT license).
#%%
import csv
import copy
import numpy as np
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
#%%

# 元のクラス。気象庁データダウンロード -> 1時間ごとのdiffusion fractionを計算。データが大きいのでインスタンスを作らない。
class Diffusion:
    @classmethod
    def jma_data(cls, ):
        




# 元のクラスを継承したCSV書き出しのクラス（引数：ファイルパス，return無し）
class CSVDiffusion(Diffusion):


# 元のクラスを継承したオブジェクト作成のクラス（return有り（pandasデータで返す？））
class ObjectDiffusion(Diffusion):
    def __init__(self):
        return()

