from markupsafe import t
import xarray
import os
import numpy as np
import time
import pickle
#from cfgrib.xarray_to_grib import to_grib



gribs = sorted(os.listdir('data/gribs'), reverse=True)
subgribs = ['data/gribs/' + x for x in gribs if x[-5:] == '.grb2' and '.hs.' in x]


if False:
    pwr_dic = {}
    for x in range(3000):
        for y in range(2500):
            pwr_dic[x, y] = (x + y)/100 

    pwr_dic[-1, -1] = np.nan
else:
    pwr_dic = pickle.load( open("data/power/mock.p", "rb" ) )
    #pwr_dic[np.nan, 0]

pwr_out = {}

x_max = 720
y_max = 361

files = os.listdir('data/power')

for year in range(2009, 1999, -1):
    if [file for file in files if 'power_glo_%s.nc' % str(year) in file]:
        continue

    year_out = np.empty((0,y_max, x_max)).astype(np.float32)
    dates_out = np.empty(0).astype(np.datetime64)
    for month in  range(1, 13):
        month = str(month).zfill(2)
        date = str(year) + month

        grib = {}
        for var in [('hs', 'swh'), ('tp', 'perpw')]:            
            url = 'data/gribs/multi_reanal.glo_30m_ext.%s.%s.grb2' % (var[0], date)
            template = xarray.open_dataset(url, engine='cfgrib')
            grib_xr = getattr(template * 100, var[1])
            grib[var[0]] = grib_xr.data.astype(np.float32)
            if var[0] == 'tp': 
                grib[var[0]] = np.nan_to_num(grib[var[0]], nan=0)
            
        month_out = np.full((len(grib['hs']), y_max, x_max), np.nan).astype(np.float32)

        t_month = time.time()
        for t in range(len(month_out)):
            t_slice = time.time()
            count_t = 0
            t_1_tot = 0
            t_2_tot = 0
            t_3_tot = 0
            t_4_tot = 0
            
            for y in range(y_max):
                row_nans = np.isnan(grib['hs'][t, y])
                if row_nans.all():
                    continue
                               
                for x, x_nan in enumerate(row_nans):
                    if x_nan:
                        continue
                    
                    month_out[t, y, x] = pwr_dic[int(grib['tp'][t, y, x] ), int(grib['hs'][t, y, x] )]
                   
            print(str(year), str(month), str(t), round(time.time() - t_slice, 3), 'seconds')
            #print('From t_1:', round(t_1_tot, 3), 'seconds')
            #print('From t_2:', round(t_2_tot, 3), 'seconds')
            #print('From t_3:', round(t_3_tot, 3), 'seconds')
            #print('From t_4:', round(t_4_tot, 3), 'seconds')
            #print('Count:', count_t)

            
        print(str(year), str(month), round(time.time() - t_month, 1), 'seconds', end='\n')
        #dates_out = np.concatenate((dates_out, template.valid_time.data))
        #year_out = np.concatenate((year_out, month_out), axis=0)

        # create dataset
        ds = xarray.Dataset({
            'power': xarray.DataArray(
                data   = month_out,   
                dims   = ['time', 'y', 'x'],
                coords = {
                    'time': template.valid_time.data,
                    'x': template.longitude.data, 
                    'y': template.latitude.data
                    
                },
                attrs = {
                    '_FillValue': -999.9,
                    'units'     : 'PWR',
                    'source': 'WaveWatch III hindcast data (hs, tp)'
                    }
            )
        })

        file_name = 'power_glo_' + str(year) + '_' + str(month) + '.nc'
        ds.to_netcdf(file_name, engine='h5netcdf', encoding={"power": {"zlib": True, "complevel": 9}})
        os.rename(file_name, 'data/power/' + file_name)




                


#ref_mask = xarray.open_dataset(subgribs[0], engine='cfgrib').swh.data[0] >= 0
#base_len = np.count_nonzero(ref_mask)

grib_merged = xarray.open_dataset(subgribs[0], engine='cfgrib')

for i, grib_url in enumerate(subgribs[1:]):
    grib = xarray.open_dataset(grib_url, engine='cfgrib')
    try:    
        grib_merged = xarray.concat([grib_merged, grib], 'step')
    except:
        input('hmm')
    #mask = grib.swh.data[0] >= 0
    #ref_mask *= mask




    


