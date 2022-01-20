import time
import os
import numpy as np
import requests
import pickle
import xarray
import pandas as pd
from multiprocessing import Pool

def parse_dates(years, months):
    names = ['years', 'months']
    dic = {'years': '', 'months': ''}
    for i, dates in enumerate([years, months]):

        if isinstance(dates, tuple):
            dic[names[i]] = list(map(str, range(*dates)))
            dic[names[i]] = [x.zfill(2) for x in dic[names[i]]]

        elif isinstance(dates, int):
            dic[names[i]] = [str(dates).zfill(2)]
        elif isinstance(dates, list):
            for date_i, date in enumerate(dates):
                dates[date_i] = str(date).zfill(2)
        else:
            assert False, 'Not list, int or tuple (' + ['years', 'months'][i] + ')'

    return dic['years'], dic['months']


class HsTp2Power:
    def __init__(self, years, months, power_file, geo='glo', res='30m', dec=2, paths={'gribs': 'data/gribs/', 'power': 'data/power/'}):
        years, months = parse_dates(years, months)
        self.paths = paths
        self.power_dic = self.load_power_dic(power_file, dec=dec)
              
        for year in years:
            for month in months:
                self.pwr_out = {}
                self.file_out = 'power_%s_%s_%s_%s.nc' % (geo, res, year, month)
                if self.file_out in os.listdir(self.paths['power']):
                    continue
                
                self.launch(year, month, geo, res, dec=dec)


    def launch(self, year, month, geo, res, dec=2):

        self.check_gribs(year, month, geo, res)
        self.load_gribs(year, month, geo, res)
        power_array = self.get_power_from_gribs()
        self.save_power(power_array)
        pass
    

    def load_power_dic(self, power_file, dec=2, max_hs=15, max_tp=40):
        if power_file.split('.')[-1] == 'p' or power_file.split('.')[0] + '.p' in os.listdir(self.paths['power']):
            return pickle.load( open(self.paths['power'] + power_file.split('.')[0] + '.p', "rb" ) )
           
        
        elif power_file.split('.')[-1] == 'csv':
            power_df = pd.read_csv(self.paths['power'] + power_file, index_col=0)
            power_dic = {}
            mult = 10 ** dec

            df = pd.DataFrame(
                [], 
                index=range(max_tp * mult), 
                columns=range(max_hs * mult)
            )

            for y_tp, row_hs in df.iterrows():
                for x_hs, _ in enumerate(row_hs):
                        if y_tp in power_df.index and str(x_hs) in power_df.columns:
                            df.at[y_tp * mult, x_hs * mult] = power_df[str(x_hs)][y_tp]

            df = df.astype(np.float32)
            df = df.interpolate()
            df = df.fillna(0)


            for y_tp, row_hs in df.iterrows():
                for x_hs, val_power in enumerate(row_hs):
                    power_dic[y_tp, x_hs] = val_power

            pick = pickle.dump(power_dic, open(self.paths['power'] + power_file.split('.')[0] + '.p', "wb" ) )
            return pick
        else:
            assert False, "Wrong power matrix provided " + power_file
        
    
    def check_gribs(self, year, month, geo, res):
        for var in ['hs', 'tp']:
            grib_name = 'multi_reanal.%s_%s_ext.%s.%s.grb2' % (geo, res, var, year + month)
            if grib_name not in os.listdir(self.paths['gribs']):
                self.get_grib(self, year, month, geo, res, var)


    def load_gribs(self, year, month, geo, res, dec=2):
        self.gribs = {}
        self.year = year
        self.month = month
        self.geo = geo
        self.res = res
        for var in [('hs', 'swh'), ('tp', 'perpw')]:            
            url = 'multi_reanal.%s_%s_ext.%s.%s.grb2' % (geo, res, var[0], year + month)
            self.gribs[var[0]] = xarray.open_dataset(self.paths['gribs'] + url, engine='cfgrib')
            if var[0] == 'hs': 
                self.time = self.gribs[var[0]].valid_time.data
                self.lat = self.gribs[var[0]].latitude.data
                self.lon = self.gribs[var[0]].longitude.data
            self.gribs[var[0]] = getattr(self.gribs[var[0]] * (10 ** dec), var[1]).data.astype(np.float32)
            if var[0] == 'tp': 
                self.gribs[var[0]] = np.nan_to_num(self.gribs[var[0]], nan=0)
            


    def get_grib(self, year, month, geo, res, var):
        date = year + month
        url = 'https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/%s/gribs/multi_reanal.%s_%s_ext.%s.%s.grb2' % (date, geo, res, var, date)
        resp = requests.get(url, allow_redirects=True)
        if resp.status_code != 200:
            input(resp.content)
        open(self.paths['gribs'] + url.split('/')[-1], 'wb').write(resp.content)


    def get_power_from_gribs(self):
        y_max, x_max = self.gribs['hs'].shape[1:3]
        
        self.power_array = np.full((len(self.gribs['hs']), y_max, x_max), np.nan).astype(np.float32)
        self.nan_slice = np.full((y_max, x_max), np.nan).astype(np.float32)
        
        #with Pool(5) as p:
        #    p.map(self.process_gribs_slice, range(len(self.power_array)))
        for t in range(len(self.power_array)):
            self.process_gribs_slice(t)

        

    def process_gribs_slice(self, t):
        t_1 = time.time()
        for y in range(len(self.nan_slice)):
            row_nans = np.isnan(self.gribs['hs'][t, y])
            if row_nans.all():
                continue
                            
            for x, x_nan in enumerate(row_nans):
                if x_nan:
                    continue
                
                self.power_array[t, y, x] = self.power_dic[int(self.gribs['tp'][t, y, x]), int(self.gribs['hs'][t, y, x])]
        print(self.year, self.month, t, np.round(time.time() - t_1, 3))
        


    def save_power(self, power_array):
        ds = xarray.Dataset({
            'power': xarray.DataArray(
                data   = power_array,   
                dims   = ['time', 'y', 'x'],
                coords = {
                    'time': self.time,
                    'x': self.lon, 
                    'y': self.lat
                    
                },
                attrs = {
                    '_FillValue': -999.9,
                    'units'     : 'PWR',
                    'source': 'WaveWatch III hindcast data (hs, tp)'
                    }
            )
        })

        file_name = 'power_' + self.geo + '_' + self.res + '_' + self.year + '_' + self.month + '.nc'
        ds.to_netcdf(file_name, engine='h5netcdf', encoding={"power": {"zlib": True, "complevel": 9}})
        os.rename(file_name, self.paths['power'] + file_name)


if __name__ == "__main__":
    years = (2007, 2000, -1)
    months = (1,12)
    power_file = 'power_matrix.csv'

    HsTp2Power(years, months, power_file)
    