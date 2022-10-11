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


### reconcile inputs, configs, etc. with below as well


### MOVE TO INPUT PROCESSING
@orca.step()
def regional_pois(settings, landmarks):
    # because of the aforementioned limit of one netowrk at a time for the POIS, as well as the large amount of memory used, 
    # this is now a preprocessing step
    n = make_network(settings['build_networks']['drive']['name'], "CTIMEV", 75)

    n.init_pois(num_categories=1, max_dist=75, max_pois=1)

    cols = {}
    for locname in ["embarcadero", "stanford", "pacheights"]:
        locs = landmarks.local.query("name == '%s'" % locname)
        n.set_pois("tmp", locs.lng, locs.lat)
        cols[locname] = n.nearest_pois(75, "tmp", num_pois=1)[1]

    df = pd.DataFrame(cols)
    print(df.describe())
    df.index.name = "tmnode_id"
    df.to_csv('regional_poi_distances.csv')


@orca.step()
def local_pois(settings):
    # because of the aforementioned limit of one netowrk at a time for the POIS, as well as the large amount of memory used, 
    # this is now a preprocessing step
    n = make_network(model_settings['build_networks']['walk']['name'], "weight", 3000)

    n.init_pois(num_categories=1, max_dist=3000, max_pois=1)

    cols = {}

    locations = pd.read_csv(os.path.join(misc.data_dir(), 'bart_stations.csv'))
    n.set_pois("tmp", locations.lng, locations.lat)
    cols["bartdist"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

    locname = 'pacheights'
    locs = orca.get_table('landmarks').local.query("name == '%s'" % locname)
    n.set_pois("tmp", locs.lng, locs.lat)
    cols["pacheights"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

    df = pd.DataFrame(cols)
    df.index.name = "node_id"
    df.to_csv('local_poi_distances.csv')
###


def make_network(name, weight_col, max_distance):
    st = pd.HDFStore(os.path.join(misc.data_dir(), name), "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"], edges[[weight_col]])
    net.precompute(max_distance)
    return net


def make_network_from_settings(model_settings):
    return make_network(model_settings["name"], settings.get("weight_col", "weight"),settings['max_distance'])


@orca.injectable(cache=True)
def net(model_settings):
    nets = {}
    pdna.reserve_num_graphs(len(settings["build_networks"]))

    # hardcoded since we can only do nearest queries on the first graph I initialize due to limitation in pandana
    for key in settings["build_networks"].keys():
        nets[key] = make_network_from_settings(settings['build_networks'][key])

    return nets


@orca.step()
def neighborhood_vars(net):
    nodes = networks.from_yaml(net["walk"], "neighborhood_vars.yaml")
    nodes = nodes.replace(-np.inf, np.nan)
    nodes = nodes.replace(np.inf, np.nan)
    nodes = nodes.fillna(0)

    print(nodes.describe())
    orca.add_table("nodes", nodes)


@orca.step()
def regional_vars(net):
    nodes = networks.from_yaml(net["drive"], "regional_vars.yaml")
    nodes = nodes.fillna(0)

    nodes2 = pd.read_csv('../inputs/basis/pandana_accessibility/regional_poi_distances.csv', index_col="tmnode_id")
    nodes = pd.concat([nodes, nodes2], axis=1)

    print(nodes.describe())
    orca.add_table("tmnodes", nodes)


@orca.step()
def price_vars(net):
    nodes2 = networks.from_yaml(net["walk"], "price_vars.yaml")
    nodes2 = nodes2.fillna(0)
    print(nodes2.describe())
    nodes = orca.get_table('nodes')
    nodes = nodes.to_frame().join(nodes2)
    orca.add_table("nodes", nodes)
