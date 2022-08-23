import time
import os
import numpy as np
import requests
import pickle
import xarray
import pandas as pd
import sys
from tools import parse_dates


class HsTp2Power:
    def __init__(self, years, months, power_file, geo='glo', res='30m', dec=2, paths={'gribs': 'data/gribs/', 'power': 'data/power/'}):
        years, months = parse_dates(years, months)
        self.paths = paths
        self.power_file = power_file
        #input('continue')
        self.load_power_dic(power_file, dec=dec)
        #input('continue')
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
        self.get_power_from_gribs()
        self.save_power()
        pass
    

    def load_power_dic(self, power_file, dec=2, max_hs=0, max_tp=0):
        self.max_tp = (max_tp + 5) * (10 ** dec) if max_tp > 40 else 40 * (10 ** dec)
        self.max_hs = (max_hs + 2) * (10 ** dec) if max_hs > 20 else 20 * (10 ** dec)
        if power_file.split('.')[-1] == 'p' or power_file.split('.')[0] + '.p' in os.listdir(self.paths['power']):
            self.power_dic = pickle.load( open(self.paths['power'] + power_file.split('.')[0] + '.p', "rb" ) )

        elif power_file.split('.')[-1] == 'csv':
            power_source = pd.read_csv(self.paths['power'] + power_file, index_col=0)
            power_source.columns = list(map((lambda x: str(int(float(x) * (10 ** dec)))), power_source.columns))
            power_source.index = list(map(lambda x: x * (10 ** dec), power_source.index))

            self.power_dic = {}

            df = pd.DataFrame(
                [], 
                index=range(self.max_tp), 
                columns=range(self.max_hs)
            )

            for y_tp, row_hs in df.iterrows():
                for x_hs, _ in enumerate(row_hs):
                        if y_tp in power_source.index and str(x_hs) in power_source.columns:
                            df.at[y_tp, x_hs] = power_source[str(x_hs)][y_tp]

            df = df.astype(np.float32)
            df = self.interpolate_df(df, power_source, dec=dec)
            df = df.fillna(0)

            ###
            self.power_df = df

            for y_tp, row_power in df.iterrows():
                for x_hs, val_power in row_power.items():
                    self.power_dic[y_tp, x_hs] = val_power

            pickle.dump(self.power_dic, open(self.paths['power'] + power_file.split('.')[0] + '.p', "wb" ) )
     
        else:
            assert False, "Wrong power matrix provided " + power_file
        
    
    def check_gribs(self, year, month, geo, res):
        for var in ['hs', 'tp']:
            grib_name = 'multi_reanal.%s_%s_ext.%s.%s.grb2' % (geo, res, var, year + month)
            if grib_name not in os.listdir(self.paths['gribs']):
                self.get_grib(year, month, geo, res, var)


    def interpolate_df(self, df, power_source, dec=2):
        mult = 10 ** dec
        for source_y, source_row in power_source[:-1].iterrows():
            source_y = int(source_y)

            cols = list(map(int, power_source.columns))
            rows = list(map(int, power_source.index))
            for source_x, power in source_row[:-1].items():
                source_x = int(source_x)
                
                if not source_y and not source_x:
                    step_x = int(source_row.index[1])
                    step_y = int(power_source.index[1])
                # Under shape (top_left, top_right, bot_left, bot_right)
                corners = (
                    power, 
                    power_source[str(source_x + step_x)][int(source_y)], 
                    power_source[str(source_x + step_x)][int(source_y + step_y)], 
                    power_source[str(source_x)][int(source_y + step_y)]
                )

                if source_x == 201 and source_y == 401:
                    print()
                
                for y in range(step_y):
                    for x in range(step_x): 
                        if (x, y) not in [(0, 0), (step_x, 0), (step_x, step_x), (0, step_y)]:
                            df.at[source_y + y, source_x + x] = self.interpolate_in_square(corners, (x, y), step_x=step_x, step_y=step_y)
        return df


    def interpolate_in_square(self, corners, point, step_x='', step_y=''):
        if step_x == '' or step_y == '':
            assert False, 'ERROR: interpolate_in_square(): no step provided'

        f_x1 = (step_x - point[0]) / step_x * corners[3] + point[0] / step_x * corners[2] 
        f_x2 = (step_x - point[0]) / step_x * corners[0] + point[0] / step_x * corners[1] 

        interpolated = (step_y - point[1]) / step_y * f_x2 + point[1] / step_y * f_x1

        return interpolated


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
        for t in range(self.power_array.shape[0]):
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
                
                try:
                    self.power_array[t, y, x] = self.power_dic[int(self.gribs['tp'][t, y, x]), int(self.gribs['hs'][t, y, x])]
                    
                except KeyError:
                    os.remove(self.paths['power'] + '.'.join(self.power_file.split('.')[:-1]) + '.p')
                    dic = {'power_file': self.power_file}
                    if y > self.max_tp:
                        dic['max_tp'] = y + 2
                    if x > self.max_hs:
                        dic['max_hs'] = x + 2
                    self.load_power_dic(**dic)
                except IndexError:
                    print('IndexError (t, x, y):', t, x, y)
        print(self.year, self.month, t, np.round(time.time() - t_1, 3))
      

    def save_power(self):
        ds = xarray.Dataset({
            'power': xarray.DataArray(
                data   = self.power_array.astype(np.float32),   
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
    if len(sys.argv) == 3:
        years = sys.argv[1]
        if len(years.split(',')) > 1:
            years = (int(years.split(',')[0]), int(years.split(',')[1]))
        months = sys.argv[2]
        if len(months.split(',')) > 1:
            months = (int(months.split(',')[0]), int(months.split(',')[1]))
    elif len(sys.argv) == 2:
        years = sys.argv[1]
        if len(years.split(',')) > 1:
            years = (int(years.split(',')[0]), int(years.split(',')[1]))
        months = (1, 13)
    elif len(sys.argv) == 1:
        print('As YYYY or YYYY_min,YY_max')
        years = input('Years: ')
        months = input('Months: ')
    else:
        assert False, 'ERROR: Python launchable, too many arguments provided.'
    power_file = 'power_matrix.csv'

    HsTp2Power(years, months, power_file)
    