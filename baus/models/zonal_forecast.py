@orca.table(cache=True)
def taz2_forecast_inputs(regional_demographic_forecast):
    t2fi = pd.read_csv(os.path.join(misc.data_dir(), "taz2_forecast_inputs.csv"), dtype={'TAZ': np.int64}, index_col='TAZ').replace('#DIV/0!', np.nan)

    rdf = regional_demographic_forecast.to_frame()
    # apply regional share of hh by size to MAZs with no households in 2010
    t2fi.loc[t2fi.shrw0_2010.isnull(), 'shrw0_2010'] = rdf.loc[rdf.year == 2010, 'shrw0'].values[0]
    t2fi.loc[t2fi.shrw1_2010.isnull(), 'shrw1_2010'] = rdf.loc[rdf.year == 2010, 'shrw1'].values[0]
    t2fi.loc[t2fi.shrw2_2010.isnull(), 'shrw2_2010'] = rdf.loc[rdf.year == 2010, 'shrw2'].values[0]
    t2fi.loc[t2fi.shrw3_2010.isnull(), 'shrw3_2010'] = rdf.loc[rdf.year == 2010, 'shrw3'].values[0]

    # apply regional share of persons by age category
    t2fi.loc[t2fi.shra1_2010.isnull(), 'shra1_2010'] = rdf.loc[rdf.year == 2010, 'shra1'].values[0]
    t2fi.loc[t2fi.shra2_2010.isnull(), 'shra2_2010'] = rdf.loc[rdf.year == 2010, 'shra2'].values[0]
    t2fi.loc[t2fi.shra3_2010.isnull(), 'shra3_2010'] = rdf.loc[rdf.year == 2010, 'shra3'].values[0]
    t2fi.loc[t2fi.shra4_2010.isnull(), 'shra4_2010'] = rdf.loc[rdf.year == 2010, 'shra4'].values[0]

    # apply regional share of hh by presence of children
    t2fi.loc[t2fi.shrn_2010.isnull(), 'shrn_2010'] = rdf.loc[rdf.year == 2010, 'shrn'].values[0]
    t2fi.loc[t2fi.shry_2010.isnull(), 'shry_2010'] = rdf.loc[rdf.year == 2010, 'shry'].values[0]

    t2fi[['shrw0_2010', 'shrw1_2010', 'shrw2_2010', 'shrw3_2010', 'shra1_2010', 'shra2_2010', 'shra3_2010', 'shra4_2010', 'shrn_2010',
          'shry_2010']] = t2fi[['shrw0_2010', 'shrw1_2010', 'shrw2_2010', 'shrw3_2010', 'shra1_2010', 'shra2_2010',
                                'shra3_2010', 'shra4_2010', 'shrn_2010',
                                'shry_2010']].astype('float')
    return t2fi

@orca.table(cache=True)
def maz_forecast_inputs(regional_demographic_forecast):
    rdf = regional_demographic_forecast.to_frame()
    mfi = pd.read_csv(os.path.join(misc.data_dir(), "maz_forecast_inputs.csv"), dtype={'MAZ': np.int64}, index_col='MAZ').replace('#DIV/0!', np.nan)

    # apply regional share of hh by size to MAZs with no households in 2010
    mfi.loc[mfi.shrs1_2010.isnull(), 'shrs1_2010'] = rdf.loc[rdf.year == 2010, 'shrs1'].values[0]
    mfi.loc[mfi.shrs2_2010.isnull(), 'shrs2_2010'] = rdf.loc[rdf.year == 2010, 'shrs2'].values[0]
    mfi.loc[mfi.shrs3_2010.isnull(), 'shrs3_2010'] = rdf.loc[rdf.year == 2010, 'shrs3'].values[0]
    # the fourth category here is missing the 'r' in the csv
    mfi.loc[mfi.shs4_2010.isnull(), 'shs4_2010'] = rdf.loc[rdf.year == 2010, 'shrs4'].values[0]
    mfi[['shrs1_2010', 'shrs2_2010', 'shrs3_2010',
         'shs4_2010']] = mfi[['shrs1_2010', 'shrs2_2010', 'shrs3_2010', 'shs4_2010']].astype('float')
    return mfi


def add_population(df, year, regional_controls):
    rc = regional_controls
    target = rc.totpop.loc[year] - df.gqpop.sum()
    zfi = zone_forecast_inputs()
    s = df.tothh * zfi.meanhhsize

    s = scale_by_target(s, target)  # , .15

    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df


def add_population_tm2(df, year, regional_controls):
    rc = regional_controls
    target = rc.totpop.loc[year] - df.gqpop.sum()
    s = df.hhpop
    s = scale_by_target(s, target, .15)
    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df


def scaled_ciacre(mtcc, us_outc):
    zfi = zone_forecast_inputs()
    abgc = zfi.ciacre10_abag
    sim_difference = [us_outc - mtcc][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgc + sim_difference
    return combined_acres


def scaled_resacre(mtcr, us_outr):
    zfi = zone_forecast_inputs()
    abgr = zfi.resacre10_abag
    sim_difference = [us_outr - mtcr][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgr + sim_difference
    return combined_acres


def add_age_categories(df, year, regional_controls):
# add age categories necessary for the travel model
    zfi = zone_forecast_inputs()
    rc = regional_controls

    seed_matrix = zfi[["sh_age0004", "sh_age0519", "sh_age2044", "sh_age4564", "sh_age65p"]].mul(df.totpop, axis='index').as_matrix()

    row_marginals = df.totpop.values
    agecols = ["age0004", "age0519", "age2044", "age4564", "age65p"]
    col_marginals = rc[agecols].loc[year].values

    target = df.totpop.sum()
    col_marginals = scale_by_target(pd.Series(col_marginals), target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    agedf = pd.DataFrame(mat)
    agedf.columns = [col.upper() for col in agecols]
    agedf.index = zfi.index
    for ind, row in agedf.iterrows():
        target = df.totpop.loc[ind]
        row = row.round()
        agedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in agedf.columns:
        df[col] = agedf[col]

    return df


def adjust_hhsize(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year, ['shrs1', 'shrs2', 'shrs3', 'shrs4']] * total_hh).values[0]
    row_marginals = df.hh.fillna(0).values
    seed_matrix = np.round(df[['hh_size_1', 'hh_size_2', 'hh_size_3', 'hh_size_4_plus']]).as_matrix()

    target = df.hh.sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhsizedf = pd.DataFrame(mat)

    hhsizedf.columns = ['hh_size_1', 'hh_size_2', 'hh_size_3', 'hh_size_4_plus']
    hhsizedf.index = df.index
    for ind, row in hhsizedf.iterrows():
        target = df.hh.loc[ind]
        row = row.round()
        hhsizedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhsizedf.columns:
        df[col] = hhsizedf[col]

    return df


def adjust_hhwkrs(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year, ['shrw0', 'shrw1', 'shrw2', 'shrw3']] * total_hh).values[0]
    row_marginals = df.hh.fillna(0).values
    seed_matrix = np.round(df[['hh_wrks_0', 'hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']]).as_matrix()

    target = df.hh.sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhwkrdf = pd.DataFrame(mat)
    hhwkrdf.columns = ['hh_wrks_0', 'hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']
    hhwkrdf.index = df.index
    for ind, row in hhwkrdf.iterrows():
        target = df.hh.loc[ind]
        row = row.round()
        hhwkrdf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhwkrdf.columns:
        df[col] = hhwkrdf[col]

    return df


def adjust_page(df, year, regional_controls):
    rc = regional_controls
    rc['age0019'] = rc.age0004 + rc.age0519
    col_marginals = rc.loc[year, ['age0019', 'age2044', 'age4564', 'age65p']]
    row_marginals = df['hhpop'].fillna(0).values
    seed_matrix = np.round(df[['pers_age_00_19', 'pers_age_20_34', 'pers_age_35_64', 'pers_age_65_plus']]).as_matrix()

    target = df['hhpop'].sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    pagedf = pd.DataFrame(mat)
    pagedf.columns = ['pers_age_00_19', 'pers_age_20_34', 'pers_age_35_64', 'pers_age_65_plus']
    pagedf.index = df.index
    for ind, row in pagedf.iterrows():
        target = np.round(df['hhpop'].loc[ind])
        row = row.round()
        pagedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in pagedf.columns:
        df[col] = pagedf[col]

    return df


def adjust_hhkids(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year, ['shrn', 'shry']] * total_hh).values[0]
    row_marginals = df.hh.fillna(0).values
    seed_matrix = np.round(df[['hh_kids_no', 'hh_kids_yes']]).as_matrix()

    target = df.hh.sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhkidsdf = pd.DataFrame(mat)

    hhkidsdf.columns = ['hh_kids_no', 'hh_kids_yes']
    hhkidsdf.index = df.index
    for ind, row in hhkidsdf.iterrows():
        target = np.round(df.hh.loc[ind])
        row = row.round()
        hhkidsdf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhkidsdf.columns:
        df[col] = hhkidsdf[col]

    return df


def add_employment(df, year, regional_controls):
    # add employed residents
    hhs_by_inc = df[["hhincq1", "hhincq2", "hhincq3", "hhincq4"]]
    hh_shares = hhs_by_inc.divide(hhs_by_inc.sum(axis=1), axis="index")

    zfi = zone_forecast_inputs()

    # this uses a regression with estimated coefficients done by @mkreilly
    empshare = 0.46381 * hh_shares.hhincq1 + 0.49361 * hh_shares.hhincq2 + 0.56938 * hh_shares.hhincq3 +\
               0.29818 * hh_shares.hhincq4 + zfi.zonal_emp_sh_resid10

    # clip so that no more than 70% of people can be employed in a given zone
    # this also makes sure that the employed residents is less then the total population (after scaling)
    # if the assertion below is triggered you can fix it by reducing this
    empshare = empshare.fillna(0).clip(.3, .7)
    empres = empshare * df.totpop

    rc = regional_controls
    target = rc.empres.loc[year]

    empres = scale_by_target(empres, target)
    df["empres"] = round_series_match_target(empres, target, 0)

    # this is to make the assertion below pass
    df["empres"] = df[["empres", "totpop"]].min(axis=1)
    # make sure employed residents is less than total residents
    assert (df.empres <= df.totpop).all()

    return df


# temporary function to balance households while some parcels have unassigned MAZ
def add_households(df, tothh):
    s = scale_by_target(df.tothh, tothh)  # , .15

    df["tothh"] = round_series_match_target(s, tothh, 0)
    df["tothh"] = df.tothh.fillna(0)
    return df