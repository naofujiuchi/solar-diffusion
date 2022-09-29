# -*- coding: utf-8 -*-
# Naomichi Fujiuchi (naofujiuchi@gmail.com), September 2022
# This is an original work by Fujiuchi (MIT license).
# The scraping code is originated from https://qiita.com/Cyber_Hacnosuke/items/122cec35d299c4d01f10 and https://www.gis-py.com/entry/scraping-weather-data
# The meteorological data is obtained from the website of Japan Meteorological Agency (気象庁)
#%%
import csv
import copy
import datetime
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
#%%

# 元のクラス。気象庁1時間ごとデータをダウンロード -> 1時間ごとのdiffusion fractionを計算。データが大きいのでインスタンスを作らない。
class Diffusion:
    BASE_URL = "http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_%s1.php?prec_no=%s&block_no=%s&year=%s&month=%s&day=%s&view="

    @staticmethod
    def str2float(str):
        try:
            return float(str)
        except:
            return 0.0

    @staticmethod
    def jma_data_type(prec_no, block_no):
        """
        Parameters
        ----------
        data_type: string
            "a" (amedas data) or "s" (sokkoujyo data)
        prec_no: string
            Text string of 2 digits number of each prefecture
        block_no: string
            Text string of 4 or 5 digits number of each region
        """
        data_type = 
        return(data_type)

    def jma_hourly_data_per_day(self, prec_no, block_no, year, month, day):
        """
        Parameters
        ----------
        prec_no: string
            Text string of 2 digits number of each prefecture
        block_no: string
            Text string of 4 or 5 digits number of each region
        """
        data_type = self.jma_data_type(prec_no=prec_no, block_no=block_no)
        r = requests.get(self.BASE_URL%(data_type, prec_no, block_no, year, month, day))
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text)
        trs = soup.find("table", { "class" : "data2_s" })
        date = datetime.date(year, month, day)
        data_list = []
        data_list_per_hour = []
        for tr in trs.findAll('tr')[2:]:
            tds = tr.findAll('td')
            if tds[1].string == None:
                break;
            data_list.append(date)
            data_list.append(tds[0].string)
            data_list.append(self.str2float(tds[1].string))
            data_list.append(self.str2float(tds[2].string))
            data_list.append(self.str2float(tds[3].string))
            data_list.append(self.str2float(tds[4].string))
            data_list.append(self.str2float(tds[5].string))
            data_list.append(self.str2float(tds[6].string))
            data_list.append(self.str2float(tds[7].string))
            data_list.append(self.str2float(tds[8].string))
            data_list.append(self.str2float(tds[9].string))
            data_list.append(self.str2float(tds[10].string))
            data_list.append(self.str2float(tds[11].string))
            data_list.append(self.str2float(tds[12].string))
            data_list.append(self.str2float(tds[13].string))
            data_list_per_hour.append(data_list)
            data_list = []
        return data_list_per_hour

    def jma_hourly_data(self, prec_no, block_no, start_date, end_date):  # type of start_date and end_date: datetime.date
        # Limit the duration (from start_date to end_date) less than 1 year
        fields = ["date", "hour", "pressure_ground", "pressure_sealevel", "precipitation_rain", "temperature_air", "temperature_condensation", "pressure_vapor", "humidity_relative", "wind_speed", "wind_direction", "hour_radiation", "radiation_solar", "precipitation_snow", "height_snow"]
        # pressure_ground [hPa], pressure_sealevel [hPa], precipitation_rain [mm], temperature_air [C], temperature_condensation [C], pressure_vapor [hPa], humidity_relative [%], wind_speed [m s-1], wind_direction, hour_radiation [h], radiation_solar [MJ m-2], precipitation_snow [cm], height_snow [cm]
        all_data = []
        all_data.append(fields)
        date = start_date
        while date != end_date + datetime.timedelta(1):
            hourly_data = self.jma_hourly_data_per_day(prec_no, block_no, year=date.year, month=date.month, day=date.day)
            for hd in hourly_data:
                all_data.append(hd)
            date += datetime.timedelta(1)
        return all_data

    # 入力されたprec_noおよびblock_noをもとにcsvからその地の緯度・経度を取得し，散乱光割合を計算
    def 

# # 元のクラスを継承したCSV書き出しのクラス（引数：ファイルパス，return無し）
# class CSVDiffusion(Diffusion):
    def write_csv(self, file, prec_no, block_no, start_date, end_date):
        all_data = self.jma_hourly_data(prec_no, block_no, start_date, end_date)
        with open(file, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerows(all_data)

# # 元のクラスを継承したオブジェクト作成のクラス（return有り（pandasデータで返す？））
# class ObjectDiffusion(Diffusion):
    def return_table(self, prec_no, block_no, start_date, end_date):
        all_data = self.jma_hourly_data(prec_no, block_no, start_date, end_date)
        return(all_data)
