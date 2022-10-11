from __future__ import print_function
import os
import sys
import yaml
import numpy as np
import pandas as pd
import orca
import pandana.network as pdna
from urbansim.developer import sqftproforma
from urbansim.developer.developer import Developer as dev
from urbansim.utils import misc, networks
from urbansim_defaults import models, utils
from baus import datasources, subsidies, summaries, variables
from baus.utils import add_buildings, groupby_random_choice, parcel_id_to_geom_id, round_series_match_target


@orca.step()
def scheduled_development_events(buildings, development_projects, demolish_events, summary, year, parcels,
                                 mapping, years_per_iter, parcels_geography, building_sqft_per_job, vmt_fee_categories,
                                 static_parcels, base_year):
    # add projects from the simulation year and previous four years, 
    # the base year is treated differently e.g., 2015 adds projects built 2010-2015
    
    # demolish existing buildings and unplace agents
    if year == (base_year + years_per_iter):
        demolish = demolish_events.to_frame().query("%d <= year_built <= %d" % (year - years_per_iter, year))
    else:
        demolish = demolish_events.to_frame().query("%d < year_built <= %d" % (year - years_per_iter, year))
    print("Demolishing/building %d buildings" % len(demolish))
    l1 = len(buildings)
    buildings = utils._remove_developed_buildings(buildings.to_frame(buildings.local_columns), demolish,
                                                  unplace_agents=["households", "jobs"])

    orca.add_injectable('static_parcels', np.append(static_parcels, demolish.loc[demolish.action == 'build', 'parcel_id']))
    orca.add_table("buildings", buildings)
    buildings = orca.get_table("buildings")
    print("Demolished %d buildings" % (l1 - len(buildings)))

    # then build development pipeline projects
    if year == (base_year + years_per_iter):
        dps = development_projects.to_frame().query("%d <= year_built <= %d" % (year - years_per_iter, year))
    else:
        dps = development_projects.to_frame().query("%d < year_built <= %d" % (year - years_per_iter, year))
    if len(dps) == 0:
        return

    new_buildings = utils.scheduled_development_events(buildings, dps, remove_developed_buildings=False, 
                                                       unplace_agents=['households', 'jobs'])
    new_buildings["form"] = new_buildings.building_type.map(mapping['building_type_map']).str.lower()
    new_buildings["job_spaces"] = new_buildings.non_residential_sqft / new_buildings.building_type.fillna("OF").map(building_sqft_per_job)
    new_buildings["job_spaces"] = new_buildings.job_spaces.fillna(0).astype('int')
    new_buildings["geom_id"] = parcel_id_to_geom_id(new_buildings.parcel_id)
    new_buildings["SDEM"] = True
    new_buildings["subsidized"] = False
    new_buildings["zone_id"] = misc.reindex(parcels.zone_id, new_buildings.parcel_id)
    new_buildings["vmt_res_cat"] = misc.reindex(vmt_fee_categories.res_cat, new_buildings.zone_id)
    new_buildings["vmt_nonres_cat"] = misc.reindex(vmt_fee_categories.nonres_cat, new_buildings.zone_id)
    del new_buildings["zone_id"]

    summary.add_parcel_output(new_buildings)


@orca.step()
def residential_developer(feasibility, households, buildings, parcels, year, settings, summary, form_to_btype_func,
                          add_extra_columns_func, parcels_geography, limits_settings, final_year, regional_controls):

    kwargs = developer_settings['residential_developer']
    target_vacancy = regional_controls.to_frame().loc[year].st_res_vac

    num_units = dev.compute_units_to_build(len(households), buildings["residential_units"].sum(), target_vacancy)

    targets = []
    typ = "Residential"
    # apply job caps
    if typ in limits_settings:

        geog = parcels_geography.geog.reindex(parcels.index).fillna('Other')
        geog_list = limits_settings[typ].keys()

        for juris, limit in limits_settings[typ].items():

            # the target is the limit times the number of years run so far in the simulation (plus this year), minus the amount
            # built in previous years - in other words, you get rollover and development is lumpy
            current_total = parcels.total_residential_units[(juris_name == juris) & (parcels.newest_building >= 2010)].sum()
            target = (year - 2010 + 1) * limit - current_total
            # make sure we don't overshoot the total development of the limit
            # for the horizon year - for instance, in Half Moon Bay we have
            # a very low limit and a single development in a far out year can
            # easily build over the limit for the total simulation
            max_target = (final_year - 2010 + 1) * limit - current_total

            if target <= 0:
                continue

            targets.append((juris_name == juris, target, max_target, juris))
            num_units -= target

        # other cities not in the targets get the remaining target
        targets.append((~juris_name.isin(juris_list), num_units, None, "none"))

    else:
        # otherwise use all parcels with total number of units
        targets.append((parcels.index == parcels.index, num_units, None, "none"))

    for parcel_mask, target, final_target, juris in targets:

        print("Running developer for %s with target of %d" %
              (str(juris), target))

        # this was a fairly heinous bug - have to get the building wrapper
        # again because the buildings df gets modified by the run_developer
        # method below
        buildings = orca.get_table('buildings')
        print('Stats of buildings before run_developer(): \
            \n{}'.format(buildings.to_frame()[['deed_restricted_units','preserved_units','inclusionary_units']].sum()))
        new_buildings = utils.run_developer("residential", households, buildings,
            "residential_units", parcels.parcel_size[parcel_mask], parcels.ave_sqft_per_unit[parcel_mask],
            parcels.total_residential_units[parcel_mask], feasibility, year=year, form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func, num_units_to_build=int(target), 
            profit_to_prob_func=subsidies.profit_to_prob_func, **kwargs)
        print('Stats of buildings before run_developer(): \n{}'.format(
             buildings.to_frame()[['deed_restricted_units','preserved_units','inclusionary_units']].sum()))

        buildings = orca.get_table('buildings')

        if new_buildings is not None:
            new_buildings["subsidized"] = False

        if final_target is not None and new_buildings is not None:
            # make sure we don't overbuild the target for the whole simulation
            overshoot = new_buildings.net_units.sum() - final_target

            if overshoot > 0:
                index = new_buildings.tail(1).index[0]
                index = int(index)
                # make sure we don't get into a negative unit situation
                current_units = buildings.local.loc[index, "residential_units"]
                # only can reduce by as many units as we have
                overshoot = min(overshoot, current_units)
                # used below - this is the pct we need to reduce the building
                overshoot_pct = (current_units - overshoot) / float(current_units)

                buildings.local.loc[index, "residential_units"] -= overshoot

                # we also need to fix the other columns so they make sense
                for col in ["residential_sqft", "building_sqft", "deed_restricted_units", "inclusionary_units", "subsidized_units"]:
                    val = buildings.local.loc[index, col]
                    # reduce by pct but round to int
                    buildings.local.loc[index, col] = int(val * overshoot_pct)
                # also fix the corresponding columns in new_buildings
                for col in ["residential_sqft","building_sqft", "residential_units", "deed_restricted_units", "inclusionary_units", "subsidized_units"]:
                    val = new_buildings.loc[index, col]
                    new_buildings.loc[index, col] = int(val * overshoot_pct)
                for col in ["policy_based_revenue_reduction", "max_profit"]:
                    val = new_buildings.loc[index, col]
                    new_buildings.loc[index, col] = val * overshoot_pct

        summary.add_parcel_output(new_buildings)


@orca.step()
def retail_developer(jobs, buildings, parcels, nodes, feasibility, settings, summary, add_extra_columns_func, net):

    dev_settings = developer_settings['non_residential_developer']
    all_units = dev.compute_units_to_build(len(jobs), buildings.job_spaces.sum(), dev_settings['kwargs']['target_vacancy'])

    target = all_units * float(dev_settings['type_splits']["Retail"])
    # target here is in sqft
    target *= settings["building_sqft_per_job"]["HS"]

    feasibility = feasibility.to_frame().loc[:, "retail"]
    feasibility = feasibility.dropna(subset=["max_profit"])

    feasibility["non_residential_sqft"] = feasibility.non_residential_sqft.astype("int")

    feasibility["retail_ratio"] = parcels.retail_ratio
    feasibility = feasibility.reset_index()

    # create features
    f1 = feasibility.retail_ratio / feasibility.retail_ratio.max()
    f2 = feasibility.max_profit / feasibility.max_profit.max()

    # combine features in probability function - it's like combining expense
    # of building the building with the market in the neighborhood
    p = f1 * 1.5 + f2
    p = p.clip(lower=1.0/len(p)/10)

    print("Attempting to build {:,} retail sqft".format(target))

    # order by weighted random sample
    feasibility = feasibility.sample(frac=1.0, weights=p)
    bldgs = buildings.to_frame(buildings.local_columns + ["general_type"])
    devs = []

    for dev_id, d in feasibility.iterrows():

        if target <= 0:
            break

        # any special logic to filter these devs?
        # remove new dev sqft from target
        target -= d.non_residential_sqft

        # add redeveloped sqft to target
        filt = "general_type == 'Retail' and parcel_id == %d" % d["parcel_id"]
        target += bldgs.query(filt).non_residential_sqft.sum()

        devs.append(d)

    if len(devs) == 0:
        return

    # record keeping - add extra columns to match building dataframe
    # add the buidings and demolish old buildings, and add to debug output
    devs = pd.DataFrame(devs, columns=feasibility.columns)

    print("Building {:,} retail sqft in {:,} projects".format(devs.non_residential_sqft.sum(), len(devs)))
    if target > 0:
        print("   WARNING: retail target not met")

    devs["form"] = "retail"
    devs = add_extra_columns_func(devs)

    add_buildings(buildings, devs)

    summary.add_parcel_output(devs)


@orca.step()
def office_developer(feasibility, jobs, buildings, parcels, year, settings, summary, form_to_btype_func,
                     add_extra_columns_func, parcels_geography, limits_settings):

    dev_settings = settings['non_residential_developer']

    # I'm going to try a new way of computing this because the math the other
    # way is simply too hard.  Basically we used to try and apportion sectors
    # into the demand for office, retail, and industrial, but there's just so
    # much dirtyness to the data, for instance 15% of jobs are in residential
    # buildings, and 15% in other buildings, it's just hard to know how much
    # to build, we I think the right thing to do is to compute the number of
    # job spaces that are required overall, and then to apportion that new dev
    # into the three non-res types with a single set of coefficients
    all_units = dev.compute_units_to_build(
        len(jobs),
        buildings.job_spaces.sum(),
        dev_settings['kwargs']['target_vacancy'])

    print("Total units to build = %d" % all_units)
    if all_units <= 0:
        return

    for typ in ["Office"]:

        print("\nRunning for type: ", typ)

        num_units = all_units * float(dev_settings['type_splits'][typ])

        targets = []
        # now apply limits - limits are assumed to be yearly, apply to an
        # entire jurisdiction and be in terms of residential_units or
        # job_spaces
        if year > 2015 and typ in limits_settings:

            juris_name = parcels_geography.juris_name.\
                reindex(parcels.index).fillna('Other')

            juris_list = limits_settings[typ].keys()
            for juris, limit in limits_settings[typ].items():

                # the actual target is the limit times the number of years run
                # so far in the simulation (plus this year), minus the amount
                # built in previous years - in other words, you get rollover
                # and development is lumpy

                current_total = parcels.total_job_spaces[
                    (juris_name == juris) &
                    (parcels.newest_building > 2015)].sum()

                target = (year - 2015 + 1) * limit - current_total

                if target <= 0:
                    print("Already met target for juris = %s" % juris)
                    print("    target = %d, current_total = %d" % (target, current_total))
                    continue

                targets.append((juris_name == juris, target, juris))
                num_units -= target

            # other cities not in the targets get the remaining target
            targets.append((~juris_name.isin(juris_list), num_units, "none"))

        else:
            # otherwise use all parcels with total number of units
            targets.append((parcels.index == parcels.index, num_units, "none"))

        for parcel_mask, target, juris in targets:

            print("Running developer for %s with target of %d" %
                  (str(juris), target))
            print("Parcels in play:\n", pd.Series(parcel_mask).value_counts())

            # this was a fairly heinous bug - have to get the building wrapper
            # again because the buildings df gets modified by the run_developer
            # method below
            buildings = orca.get_table('buildings')

            new_buildings = utils.run_developer(typ.lower(), jobs, buildings, "job_spaces",
                                                parcels.parcel_size[parcel_mask], parcels.ave_sqft_per_unit[parcel_mask],
                                                parcels.total_job_spaces[parcel_mask], feasibility, year=year,
                                                form_to_btype_callback=form_to_btype_func,
                                                add_more_columns_callback=add_extra_columns_func, residential=False,
                                                num_units_to_build=int(target), profit_to_prob_func=subsidies.profit_to_prob_func,
                                                **dev_settings['kwargs'])

            if new_buildings is not None:
                new_buildings["subsidized"] = False

            summary.add_parcel_output(new_buildings)


@orca.step()
def developer_reprocess(buildings, year, years_per_iter, jobs,
                        parcels, summary, parcel_is_allowed_func):
    # this takes new units that come out of the developer, both subsidized
    # and non-subsidized and reprocesses them as required - please read
    # comments to see what this means in detail

    # 20% of base year buildings which are "residential" have job spaces - I
    # mean, there is a ratio of job spaces to res units in residential
    # buildings of 1 to 5 - this ratio should be kept for future year
    # buildings
    s = buildings.general_type == "Residential"
    res_units = buildings.residential_units[s].sum()
    job_spaces = buildings.job_spaces[s].sum()

    to_add = res_units * .05 - job_spaces
    if to_add > 0:
        print("Adding %d job_spaces" % to_add)
        res_units = buildings.residential_units[s]
        # bias selection of places to put job spaces based on res units
        print(res_units.describe())
        print(res_units[res_units < 0])
        add_indexes = np.random.choice(res_units.index.values, size=to_add, replace=True, p=(res_units/res_units.sum()))
        # collect same indexes
        add_indexes = pd.Series(add_indexes).value_counts()
        # this is sqft per job for residential bldgs
        add_sizes = add_indexes * 400
        print("Job spaces in res before adjustment: ", buildings.job_spaces[s].sum())
        buildings.local.loc[add_sizes.index, "non_residential_sqft"] += add_sizes.values
        print("Job spaces in res after adjustment: ", buildings.job_spaces[s].sum())

    # the second step here is to add retail to buildings that are greater than
    # X stories tall - presumably this is a ground floor retail policy
    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = old_buildings.query('%d == year_built and stories >= 4' % year)

    print("Attempting to add ground floor retail to %d devs" % len(new_buildings))
    retail = parcel_is_allowed_func("retail")
    new_buildings = new_buildings[retail.loc[new_buildings.parcel_id].values]
    print("Disallowing dev on these parcels:")
    print("    %d devs left after retail disallowed" % len(new_buildings))

    # this is the key point - make these new buildings' nonres sqft equal
    # to one story of the new buildings
    new_buildings.non_residential_sqft = new_buildings.building_sqft / new_buildings.stories * .8

    new_buildings["residential_units"] = 0
    new_buildings["residential_sqft"] = 0
    new_buildings["deed_restricted_units"] = 0
    new_buildings["inclusionary_units"] = 0
    new_buildings["subsidized_units"] = 0
    new_buildings["building_sqft"] = new_buildings.non_residential_sqft
    new_buildings["stories"] = 1
    new_buildings["building_type"] = "RB"

    # this is a fairly arbitrary rule, but we're only adding ground floor
    # retail in areas that are underserved right now - this is defined as
    # the location where the retail ratio (ratio of income to retail sqft)
    # is greater than the median
    ratio = parcels.retail_ratio.loc[new_buildings.parcel_id]
    new_buildings = new_buildings[ratio.values > ratio.median()]

    print("Adding %d sqft of ground floor retail in %d locations" % (new_buildings.non_residential_sqft.sum(), len(new_buildings)))

    all_buildings = dev.merge(old_buildings, new_buildings)
    orca.add_table("buildings", all_buildings)

    new_buildings["form"] = "retail"
    # this is sqft per job for retail use - this is all rather
    # ad-hoc so I'm hard-coding
    new_buildings["job_spaces"] = (new_buildings.non_residential_sqft / 445.0).astype('int')
    new_buildings["net_units"] = new_buildings.job_spaces
    summary.add_parcel_output(new_buildings)

    # got to get the frame again because we just added rows
    buildings = orca.get_table('buildings')
    buildings_df = buildings.to_frame(['year_built', 'building_sqft', 'general_type'])
    sqft_by_gtype = buildings_df.query('year_built >= %d' % year).groupby('general_type').building_sqft.sum()
    print("New square feet by general type in millions:\n", sqft_by_gtype / 1000000.0)