




@orca.step()
def elcm_simulate(jobs, buildings, aggregations, elcm_config):
    buildings.local["non_residential_rent"] = \
        buildings.local.non_residential_rent.fillna(0)
    return utils.lcm_simulate(elcm_config, jobs, buildings, aggregations,
                              "building_id", "job_spaces",
                              "vacant_job_spaces", cast=True)

@orca.table(cache=True)
def employment_relocation_rates():
    df = pd.read_csv(os.path.join("data", "employment_relocation_rates.csv"))
    df = df.set_index("zone_id").stack().reset_index()
    df.columns = ["zone_id", "empsix", "rate"]
    return df


def _proportional_jobs_model(
    target_ratio,  # ratio of jobs of this sector to households
    sector,        # empsix sector
    groupby_col,   # ratio will be matched at this level of geog
    hh_df,
    jobs_df,
    locations_series,
    target_jobs=None  # pass this if you want to compute target jobs
):

    if target_jobs is None:
        # compute it if not passed
        target_jobs = hh_df[groupby_col].value_counts() * target_ratio
        target_jobs = target_jobs.astype('int')

    current_jobs = jobs_df[
        jobs_df.empsix == sector][groupby_col].value_counts()
    need_more_jobs = target_jobs - current_jobs
    need_more_jobs = need_more_jobs[need_more_jobs > 0]
    need_more_jobs_total = int(need_more_jobs.sum())

    available_jobs = \
        jobs_df.query("empsix == '%s' and building_id == -1" % sector)

    print("Need more jobs total: %d" % need_more_jobs_total)
    print("Available jobs: %d" % len(available_jobs))

    if len(available_jobs) == 0:
        # corner case
        return pd.Series()

    if len(available_jobs) >= need_more_jobs_total:

        # have enough jobs to assign, truncate available jobs
        available_jobs = available_jobs.head(need_more_jobs_total)

    else:

        # don't have enough jobs - random sample locations to partially
        # match the need (won't succed matching the entire need)
        need_more_jobs = round_series_match_target(
            need_more_jobs, len(available_jobs), 0)
        need_more_jobs_total = need_more_jobs.sum()

    assert need_more_jobs_total == len(available_jobs)

    if need_more_jobs_total <= 0:
        return pd.Series()

    print("Need more jobs\n", need_more_jobs)

    excess = need_more_jobs.sub(locations_series.value_counts(), fill_value=0)
    print("Excess demand\n", excess[excess > 0])

    # there's an issue with groupby_random_choice where it can't choose from
    # a set of locations that don't exist - e.g. we have 2 jobs in a certain
    # city but not locations to put them in.  we need to drop this demand
    drop = need_more_jobs.index.difference(locations_series.unique())
    print("We don't have any locations for these locations:\n", drop)
    need_more_jobs = need_more_jobs.drop(drop).astype('int')

    # choose random locations within jurises to match need_more_jobs totals
    choices = groupby_random_choice(locations_series, need_more_jobs,
                                    replace=True)

    # these might not be the same length after dropping a few lines above
    available_jobs = available_jobs.head(len(choices))

    return pd.Series(choices.index, available_jobs.index)


@orca.step()
def proportional_elcm(jobs, households, buildings, parcels,
                      year, run_number):

    juris_assumptions_df = pd.read_csv(os.path.join(
        "data",
        "juris_assumptions.csv"
    ), index_col="juris")

    # not a big fan of this - jobs with building_ids of -1 get dropped
    # by the merge so you have to grab the columns first and fill in
    # juris iff the building_id is != -1
    jobs_df = jobs.to_frame(["building_id", "empsix"])
    df = orca.merge_tables(
        target='jobs',
        tables=[jobs, buildings, parcels],
        columns=['juris', 'zone_id'])
    jobs_df["juris"] = df["juris"]
    jobs_df["zone_id"] = df["zone_id"]

    hh_df = orca.merge_tables(
        target='households',
        tables=[households, buildings, parcels],
        columns=['juris', 'zone_id', 'county'])

    # the idea here is to make sure we don't lose local retail and gov't
    # jobs - there has to be some amount of basic services to support an
    # increase in population

    buildings_df = orca.merge_tables(
        target='buildings',
        tables=[buildings, parcels],
        columns=['juris', 'zone_id', 'general_type', 'vacant_job_spaces'])

    buildings_df = buildings_df.rename(columns={
      'zone_id_x': 'zone_id', 'general_type_x': 'general_type'})

    # location options are vacant job spaces in retail buildings - this will
    # overfill certain location because we don't have enough space
    building_subset = buildings_df[buildings_df.general_type == "Retail"]
    location_options = building_subset.juris.repeat(
        building_subset.vacant_job_spaces.clip(0))

    print("Running proportional jobs model for retail")

    s = _proportional_jobs_model(
        # we now take the ratio of retail jobs to households as an input
        # that is manipulable by the modeler - this is stored in a csv
        # per jurisdiction
        juris_assumptions_df.minimum_forecast_retail_jobs_per_household,
        "RETEMPN",
        "juris",
        hh_df,
        jobs_df,
        location_options
    )

    jobs.update_col_from_series("building_id", s, cast=True)

    # first read the file from disk - it's small so no table source
    taz_assumptions_df = pd.read_csv(os.path.join(
        "data",
        "taz_growth_rates_gov_ed.csv"
    ), index_col="Taz")

    # we're going to multiply various aggregations of populations by factors
    # e.g. high school jobs are multiplied by county pop and so forth - this
    # is the dict of the aggregations of household counts
    mapping_d = {
        "TAZ Pop": hh_df["zone_id"].dropna().astype('int').value_counts(),
        "County Pop": taz_assumptions_df.County.map(
            hh_df["county"].value_counts()),
        "Reg Pop": len(hh_df)
    }
    # the factors are set up in relation to pop, not hh count
    pop_to_hh = .43

    # don't need county anymore
    del taz_assumptions_df["County"]

    # multipliers are in first row (not counting the headers)
    multipliers = taz_assumptions_df.iloc[0]
    # done with the row
    taz_assumptions_df = taz_assumptions_df.iloc[1:]

    # this is weird but Pandas was giving me a strange error when I tried
    # to change the type of the index directly
    taz_assumptions_df = taz_assumptions_df.reset_index()
    taz_assumptions_df["Taz"] = taz_assumptions_df.Taz.astype("int")
    taz_assumptions_df = taz_assumptions_df.set_index("Taz")

    # now go through and multiply each factor by the aggregation it applied to
    target_jobs = pd.Series(0, taz_assumptions_df.index)
    for col, mult in zip(taz_assumptions_df.columns, multipliers):
        target_jobs += (taz_assumptions_df[col].astype('float') *
                        mapping_d[mult] * pop_to_hh).fillna(0)

    target_jobs = target_jobs.astype('int')

    print("Running proportional jobs model for gov/edu")

    # location options are vacant job spaces in retail buildings - this will
    # overfill certain location because we don't have enough space
    building_subset = buildings_df[
        buildings.general_type.isin(["Office", "School"])]
    location_options = building_subset.zone_id.repeat(
        building_subset.vacant_job_spaces.clip(0))

    # now do the same thing for gov't jobs
    s = _proportional_jobs_model(
        None,  # computing jobs directly
        "OTHEMPN",
        "zone_id",
        hh_df,
        jobs_df,
        location_options,
        target_jobs=target_jobs
    )

    jobs.update_col_from_series("building_id", s, cast=True)


    def proportional_job_allocation(parcel_id):
    # this method takes a parcel and increases the number of jobs on the
    # parcel in proportion to the ratio of sectors that existed in the base yr
    # this is because elcms can't get the distribution right in some cases, eg
    # to keep mostly gov't jobs in city hall, etc - these are largely
    # institutions and not subject to the market

    # get buildings on this parcel
    buildings = orca.get_table("buildings").to_frame(
        ["parcel_id", "job_spaces", "zone_id", "year_built"]).\
        query("parcel_id == %d" % parcel_id)

    # get jobs in those buildings
    all_jobs = orca.get_table("jobs").local
    jobs = all_jobs[
        all_jobs.building_id.isin(buildings.query("year_built <= 2015").index)]

    # get job distribution by sector for this parcel
    job_dist = jobs.empsix.value_counts()

    # only add jobs to new buildings records
    for index, building in buildings.query("year_built > 2015").iterrows():

        num_new_jobs = building.job_spaces - len(
            all_jobs.query("building_id == %d" % index))

        if num_new_jobs == 0:
            continue

        sectors = np.random.choice(job_dist.index, size=num_new_jobs,
                                   p=job_dist/job_dist.sum())
        new_jobs = pd.DataFrame({"empsix": sectors, "building_id": index})
        # make sure index is incrementing
        new_jobs.index = new_jobs.index + 1 + np.max(all_jobs.index.values)

        print("Adding {} new jobs to parcel {} with proportional model".format(
            num_new_jobs, parcel_id))
        print(new_jobs.head())
        all_jobs = all_jobs.append(new_jobs)
        orca.add_table("jobs", all_jobs)


@orca.step()
def static_parcel_proportional_job_allocation(static_parcels):
    for parcel_id in static_parcels:
        proportional_job_allocation(parcel_id)
