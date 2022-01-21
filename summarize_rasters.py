from tools import parse_dates
import xarray as xr
import os
import re

class Summarize:
    def __init__(self, folder='', name_re='', export=True):
        self.export = export
        if folder and name_re:
            self.path = folder
            self.files = [self.path + file for file in os.listdir(self.path) if re.search(name_re, file)]
        else:
            assert False, 'No folder and name regex provided'

    def get_statistics(self, stats, group='', years='', months='', name_str='', geo='', res='', var='power'):

        if years or months:
            years, months = parse_dates(years, months)

            files = []

            for year in years:
                for month in months:
                    file = name_str.keys()[0] % (year, month)
                    if self.path + file in self.files:
                        files = files.append(self.path + file)
        else:
            files = self.files

        mfds = xr.open_mfdataset(files).rio.write_crs('epsg:4326')
        

        if group:
            mfds= mfds.groupby('time.' + group)
        

        if isinstance(stats, str):
            stats = [stats]
        for stat in stats:
            mfds_stat = getattr(mfds, stat)(dim='time')
            for xarr in mfds_stat.power:
                index = getattr(xarr, group).values
                name = "%s_%s_%s_%s_%s.tif" % (var, stat, geo, res, index)

                if self.export:
                    xarr.rio.to_raster('data/out/' + name)
                      


if __name__ == "__main__":
    summary = Summarize(folder='data/power/', name_re='power_glo_30m_\d\d\d\d_\d\d.nc')
    mean = summary.get_statistics(
        'mean', 
        group='year',  
        geo='glo',
        res='30m',
        name_str={'power_%s_%s_%s_%s.nc': ('geo', 'res', 'years', 'months')}
    )

    