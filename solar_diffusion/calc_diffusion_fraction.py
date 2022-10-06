# -*- coding: utf-8 -*-
# Naomichi Fujiuchi (naofujiuchi@gmail.com), September 2022
# This is an original work by Fujiuchi (MIT license).
# The scraping code is originated from https://qiita.com/Cyber_Hacnosuke/items/122cec35d299c4d01f10 and https://www.gis-py.com/entry/scraping-weather-data
# The meteorological data is obtained from the website of Japan Meteorological Agency (気象庁)
#%%
import os
from math import pi
import time
import datetime
import numpy as np
import math
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pysolar
from timezonefinder import TimezoneFinder
from scipy.optimize import minimize_scalar
#%%

class Diffusion:
    BASE_URL = "http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_%s1.php?prec_no=%s&block_no=%s&year=%s&month=%s&day=%s&view="

    @staticmethod
    def str2float(str):
        try:
            return float(str)
        except:
            return 0.0

    @staticmethod
    def jma_place(prec_no, block_no, csvdir=None):
        """
        Parameters
        ----------
        prec_no: string
            Text string of 2 digits number of each prefecture
        block_no: string
            Text string of 4 or 5 digits number of each region
        csvdir: str
            Nmae of the directory in which there is jma_prec_block.csv
            This argument is used when this python code is used from R reticulate package because it can't recognize __file__
        """
        if csvdir is None:
            jp = pd.read_csv(os.path.join(os.path.dirname(__file__), 'jma_prec_block.csv'), dtype={'prec_no':'str','block_no':'str'})
        else:
            jp = pd.read_csv(os.path.join(csvdir, 'jma_prec_block.csv'), dtype={'prec_no':'str','block_no':'str'})
        jp_extract = jp.query('prec_no==@prec_no&block_no==@block_no')
        return(jp_extract)

    def jma_hourly_data_per_day(self, prec_no, block_no, year, month, day, csvdir=None):
        """
        Parameters
        ----------
        prec_no: str
            Text string of 2 digits number of each prefecture
        block_no: str
            Text string of 4 or 5 digits number of each region
        year: int
        month: int
        day: int
        """
        jma_place_extract = self.jma_place(prec_no, block_no, csvdir)
        data_type = jma_place_extract['data_type'].iloc[0]
        r = requests.get(self.BASE_URL%(data_type, prec_no, block_no, str(year), str(month).zfill(2), str(day).zfill(2)))
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text)
        trs = soup.find("table", { "class" : "data2_s" })
        date = datetime.date(year, month, day)
        data_list = []
        data_list_per_hour = []
        if data_type == "s": # sokkoujyo data
            ncol = 14
        if data_type == "a": # amedas data
            ncol = 11
        for tr in trs.findAll('tr')[2:]:
            tds = tr.findAll('td')
            if tds[1].string == None:
                break;
            data_list.append(date)
            data_list.append(tds[0].string)
            for i in range(1,ncol):
                data_list.append(self.str2float(tds[i].string))
            data_list_per_hour.append(data_list)
            data_list = []
        return data_list_per_hour # return 2D list object of hourly data in a day (i.e., 24 rows)

    def jma_hourly_data(self, prec_no, block_no, start_date, end_date, csvdir=None):
        """
        Parameters
        ----------
        prec_no: str
            Text string of 2 digits number of each prefecture
        block_no: str
            Text string of 4 or 5 digits number of each region
        start_date: datetime.date
        end_data: datetime.date
        """
        # Limit the duration (from start_date to end_date) less than 1 year
        limit_date = start_date + datetime.timedelta(days=366)
        if end_date > limit_date: 
            raise ValueError("Period over 1 year.")
        # Obtain hourly JMA data during the period from start_date to end_date
        # pressure_ground [hPa], pressure_sealevel [hPa], precipitation_rain [mm], temperature_air [C], temperature_condensation [C], pressure_vapor [hPa], humidity_relative [%], wind_speed [m s-1], wind_direction, hour_radiation [h], radiation_solar [MJ m-2], precipitation_snow [cm], height_snow [cm]
        jma_place_extract = self.jma_place(prec_no, block_no, csvdir)
        data_type = jma_place_extract['data_type'].iloc[0]
        if data_type == "s": # sokkoujyo data
            fields = ["date", "hour", "pressure_ground", "pressure_sealevel", "precipitation_rain", "temperature_air", "temperature_condensation", "pressure_vapor", "humidity_relative", "wind_speed", "wind_direction", "hour_radiation", "radiation_solar", "precipitation_snow", "height_snow"]
        if data_type == "a": # amedas data
            fields = ["date", "hour", "precipitation_rain", "temperature_air", "temperature_condensation", "pressure_vapor", "humidity_relative", "wind_speed", "wind_direction", "hour_radiation", "precipitation_snow", "height_snow"]
        all_data = []
        all_data.append(fields)
        date = start_date
        while date != end_date + datetime.timedelta(1):
            hourly_data = self.jma_hourly_data_per_day(prec_no, block_no, year=date.year, month=date.month, day=date.day, csvdir=csvdir)
            for hd in hourly_data:
                all_data.append(hd)
            date += datetime.timedelta(1)
            time.sleep(0.5) # Wait for 0.5 second everytime before accessing the next JMA data
        return all_data # return 2D list object of hourly data in a specific period

    @staticmethod
    def diffused_light(latitude, longitude, time, irradiation): 
        """
        Parameters
        ----------
        latitude: float
        longitude: float
        time: datetime.datetime (with tzinfo)
            If you have a pandas Timestamp object 'mytime', then please convert it to datetime.datetime like 'mytime.to_pydatetime()', and put the datetime.datetime object on 'time' in this function.
        irradiation: float
            solar irradiation [W m-2]
        
        Equation
        ----------
        Watanabe model for the calculation of diffused light (Urano et al., 1983; Watanabe et al., 1983).
        The coefficient of atmospheric transmission (P) can be calculated by minimizing the right hand of the last equation.
        日射の直達光と散乱光の割合を計算する。Watanabeモデル (浦野ら, 1983, 日本建築学会九州支部研究報告; 渡辺ら, 1983, 日本建築学会論文報告集) を使用する。
        最後の式の右辺が0に近づくように収束計算を行うことで大気透過率$P$を決定することができる。
        $$
        \begin{aligned}
        TH &= DN \sin h + SH \\
        DN &= I_0 P ^{\frac{1}{\sin h}} \\
        SH &= I_0 \sin h \frac{Q}{1+Q} \\
        Q &= (0.8672 + 0.7505 \sin h) P^{0.421\frac{1}{\sin h}} \left( 1 - P^{\frac{1}{\sin h}} \right) ^{2.277} \\
        TH &= I_0 P ^{\frac{1}{\sin h}} \sin h + I_0 \sin h \frac{Q}{1+Q} \\
        FRACDF &= 1 - \frac{DN\sin h}{DN\sin h + SH} \\
        &= 1 - \frac{P ^{\frac{1}{\sin h}}}{P ^{\frac{1}{\sin h}} + \frac{Q}{1+Q}} \\
        0 &= I_0 P ^{\frac{1}{\sin h}} \sin h + I_0 \sin h \frac{Q}{1+Q} - TH\\
        \end{aligned}
        $$
        $I_0$: Solar consntant 大気外法線面日射量 [W m-2]
        $P$: Coefficient of atmospheric transmission 大気透過率 [-]
        $TH$: Total horizontal solar radiation 水平面全天日射量 [W m-2]
        $DN$: Solar radiation on the normal plane 法線面直達日射量 [W m-2]
        $SH$: Diffused solar radiation on the ground 水平面天空日射量 [W m-2]
        $h$: Solar angle (Solar altitude) 太陽高度角 [rad]
        $FRACDF$: Fraction of diffused light (diffused solar radiation / total solar radiation) 散乱光割合 [-] ($0 < FRACDF < 1$)
        """
        I0 = 1366 # Solar constant [W m-2]
        h = pysolar.solar.get_altitude(latitude, longitude, time) / 180 * math.pi
        if h > 0 and irradiation > 0:
            def objective(x):
                value = abs(I0 * x**(1/math.sin(h)) * math.sin(h) + I0 * math.sin(h) * (0.8672 + 0.7505 * math.sin(h)) * x**(0.421/math.sin(h)) * (1 - x**(1/math.sin(h)))**2.277 / (1 + (0.8672 + 0.7505 * math.sin(h)) * x**(0.421/math.sin(h)) * (1 - x**(1/math.sin(h)))**2.277) - irradiation)
                return(value)
            res = minimize_scalar(objective, bounds=(0, 1), method='bounded')
            # Coefficient of atmospheric transmission [-]
            P = res.x
            # Fraction of diffused light (diffused solar radiation / total solar radiation) [-]
            FRACDF = 1 - P**(1/math.sin(h)) / (P**(1/math.sin(h)) + (0.8672 + 0.7505 * math.sin(h)) * P**(0.421/math.sin(h)) * (1 - P**(1/math.sin(h)))**2.277 / (1 + (0.8672 + 0.7505 * math.sin(h)) * P**(0.421/math.sin(h)) * (1 - P**(1/math.sin(h)))**2.277))
            return([P, FRACDF])
        else: 
            return([np.nan, np.nan])

    def output(self, prec_no, block_no, start_date, end_date, csvdir=None):
        """
        Parameters
        ----------
        prec_no: str
            Text string of 2 digits number of each prefecture
        block_no: str
            Text string of 4 or 5 digits number of each region
        start_date: datetime.date
        end_data: datetime.date

        Output
        ----------
        df: pandas.DataFrame
        """
        # Add timestamp with timezone, solar altitude, and diffusion fraction to the data table
        jma_place_extract = self.jma_place(prec_no, block_no, csvdir)
        longitude = jma_place_extract['longitude'].iloc[0]
        latitude = jma_place_extract['latitude'].iloc[0]
        tf = TimezoneFinder()  # reuse
        timezone_str = tf.timezone_at(lng=longitude, lat=latitude)
        all_data = self.jma_hourly_data(prec_no, block_no, start_date, end_date, csvdir)
        df = pd.DataFrame(all_data[1:], columns=all_data[0])
        # Because the data is the 1-hour data before the hour, timestamp is set to the time half-hour before the hour. (e.g., when hour is 1, timestamp is 0:30)
        df['year'] = df['date'].apply(lambda x: x.year)
        df['month'] = df['date'].apply(lambda x: x.month)
        df['day'] = df['date'].apply(lambda x: x.day)
        df['hour'] = df['hour'].astype(int) - 1
        df['timestamp'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-' + df['day'].astype(str) + ' ' + df['hour'].astype(str) + ':30:00')
        df.timestamp = df.timestamp.dt.tz_localize(timezone_str)
        df.radiation_solar = df.radiation_solar * 10**6 / 3600 # Convert the unit of solar radiation [MJ m-2] to [W m-2] (= [J m-2 s-1])
        lightparams = df.apply(lambda row: self.diffused_light(latitude=latitude,longitude=longitude,time=row['timestamp'].to_pydatetime(),irradiation=row['radiation_solar']), axis=1)
        df[['P','FRACDF']] = pd.DataFrame(lightparams.tolist())
        return(df)
