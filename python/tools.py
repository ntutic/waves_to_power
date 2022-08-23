
def parse_dates(years, months):
    names = ['years', 'months']
    dic = {'years': '', 'months': ''}
    for i, dates in enumerate([years, months]):

        if isinstance(dates, tuple):
            dic[names[i]] = list(map(str, range(*dates)))
            dic[names[i]] = [x.zfill(2) for x in dic[names[i]]]
        elif isinstance(dates, list):
            for date_i, date in enumerate(dates):
                dates[date_i] = str(date).zfill(2)
        else:
             dic[names[i]] = [str(dates).zfill(2)]

    return dic['years'], dic['months']
