import os, sys, time
import arcpy
import logging
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import pandas as pd
import numpy as np

NOW = time.strftime("%Y_%m%d_%H%M")

###ArcGIS portion setup
if os.getenv("USERNAME")=="MTCPB":
    WORKING_DIR         = "C:\\Users\\MTCPB\\Documents\\ArcGIS\\Projects\\Proximity"
    LOG_FILE            = os.path.join(WORKING_DIR,"proximity2transit_{}.log".format(NOW))
    P2T_GDB           = "C:\\Users\\MTCPB\\Documents\\ArcGIS\\Projects\\Proximity\\Proximity.gdb"
    WORKSPACE_GDB       = "workspace_{}.GDB".format(NOW)


MTC_ONLINE_TRANSIT15_URL = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/Existing_Transit_Stops_2020/FeatureServer/0'
MTC_ONLINE_TRANSIT50_URL = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/transitstops_01_2020_potential/FeatureServer/0'
MTC_ONLINE_BACOUNTY_URL  = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/county_region/FeatureServer/\
0?token=CjEwvAXFsqxLRQUDXcXdE2UiCvnfSdSnbwyLWY9nyEd3_X-8fqweg4FGuxjPXZ-GLsX8Hu5K_u0JfQP\
8VlapfgMcPFqFNzAkh7gjRLJnjO_a72KZtHGm3OSl-GLC3v4hqxpS478NRaSkQJB5fCLtvn_9nDiagBLbpldEwgL\
ruCJpsNDb9TPyyyrJ13Vl9LtZ-xZDqyZ5762Kju_a-nydWOqk0tB6ri9fPHh67sElVVA.'

transit_current_portal = arcpy.MakeFeatureLayer_management(MTC_ONLINE_TRANSIT15_URL,'transit_current_portal')
transit_potential_portal = arcpy.MakeFeatureLayer_management(MTC_ONLINE_TRANSIT50_URL,'transit_potential_portal')
bacounty_portal = arcpy.MakeFeatureLayer_management(MTC_ONLINE_BACOUNTY_URL,'bacounty_portal')

arcpy.env.workspace = P2T_GDB
arcpy.env.overwriteOutput = True

if arcpy.Exists('transit_current'):
    arcpy.Delete_management('transit_current')
if arcpy.Exists('transit_potential'):
    arcpy.Delete_management('transit_potential')
if arcpy.Exists('bacounty'):
    arcpy.Delete_management('bacounty')
arcpy.FeatureClassToFeatureClass_conversion(transit_current_portal, P2T_GDB,'transit_current')
arcpy.FeatureClassToFeatureClass_conversion(transit_potential_portal, P2T_GDB,'transit_potential')
arcpy.FeatureClassToFeatureClass_conversion(bacounty_portal, P2T_GDB,'bacounty')

transit_current   = os.path.join(P2T_GDB, "transit_current")
transit_potential   = os.path.join(P2T_GDB, "transit_potential")
bacounty   = os.path.join(P2T_GDB, "bacounty")

###Urbansim Setup
urbansim_run_location = 'C:/Users/{}/Box/Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/PBA50/Draft Blueprint runs/'.format(os.getenv('USERNAME'))
us_2050_DBP_Plus_runid_cleaner         = 'Blueprint Plus Crossing (s23)/v1.8 - final cleaner/run1020'
us_2050_DBP_Plus_runid_test         = 'test runs/run26' #this is for testing
list_us_runid = [us_2050_DBP_Plus_runid_cleaner, us_2050_DBP_Plus_runid_test]

if __name__ == '__main__':
    
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')

    # console handler
    ch = logging.StreamHandler()
    ch.setLevel('INFO')
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(ch)
    # file handler
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel('DEBUG')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh)

    logger.info("WORKING_DIR   = {}".format(WORKING_DIR))
    logger.info("WORKSPACE_GDB = {}".format(WORKSPACE_GDB))
    logger.info("Proximity_GDB     = {}".format(P2T_GDB))

    # list info about SMELT_GDB
    arcpy.env.workspace = P2T_GDB
    logger.info("workspace: {}".format(arcpy.env.workspace))
    for dataset in arcpy.ListDatasets():
        logger.info("  dataset: {}".format(dataset))
        logger.info("    feature classes: {} ".format(arcpy.ListFeatureClasses(feature_dataset=dataset)))
    
    logger.info("  feature classes: {} ".format(arcpy.ListFeatureClasses()))
    logger.info("  tables: {} ".format(arcpy.ListTables()))

    arcpy.CreateFileGDB_management(WORKING_DIR, WORKSPACE_GDB)
    arcpy.env.workspace = os.path.join(WORKING_DIR, WORKSPACE_GDB)

    ###doing for current transit stops
    logger.info('Select buffer for major transit stops')
    arcpy.management.SelectLayerByAttribute("transit_current", "NEW_SELECTION", "major_stop = 1", None)
    arcpy.analysis.Buffer("transit_current", "TPA2020", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    logger.info('Select buffer for headway < 15min (TRA)')
    arcpy.management.SelectLayerByAttribute("transit_current", "NEW_SELECTION", "hdwy_15min = 1", None)
    arcpy.analysis.Buffer("transit_current", "TRA2020", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    logger.info('Select buffer for headway 15-30 min')
    arcpy.management.SelectLayerByAttribute("transit_current", "NEW_SELECTION", "hdwy_30min = 1", None)
    arcpy.analysis.Buffer("transit_current", "Bus15_30", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    logger.info('Select buffer for headway 30+ min')
    arcpy.management.SelectLayerByAttribute("transit_current", "NEW_SELECTION", "hdwy_class = '31 mins or more'", None)
    arcpy.analysis.Buffer("transit_current", "Bus30Plus", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    #Using county boundaries alone did not capture all parcels (1,952,484/1,956,212). With buffering 0.5 mi 1,956,044/1,956,212 99.99% of all parcels
    arcpy.analysis.Buffer('bacounty', "bacounty_expand", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    logger.info('only include high-freq bus area not already part of Major Transit Stop buffer')
    arcpy.analysis.Erase("TRA2020", "TPA2020", "TRA2020_Erased", None)

    logger.info('Create Bus 15-30min shape')
    arcpy.analysis.Erase("Bus15_30", "TRA2020_Erased", "Bus15_30_PartialErased", None)
    arcpy.analysis.Erase("Bus15_30_PartialErased", "TPA2020", "Bus15_30_Erased", None)

    logger.info('Create Bus 31+ min shape')
    arcpy.analysis.Erase("Bus30Plus", "Bus15_30_Erased", "Bus30Plus_PartialErased1", None)
    arcpy.analysis.Erase("Bus30Plus_PartialErased1", "TRA2020_Erased", "Bus30Plus_PartialErased2", None)
    arcpy.analysis.Erase("Bus30Plus_PartialErased2", "TPA2020", "Bus30Plus_Erased", None)

    logger.info('Create Rest of Bay Area Shape')
    arcpy.analysis.Erase("BAcounty_expand", "Bus30Plus_Erased", "RestofBA_PartialErased1", None)
    arcpy.analysis.Erase("RestofBA_PartialErased1", "Bus15_30_Erased", "RestofBA_PartialErased2", None)
    arcpy.analysis.Erase("RestofBA_PartialErased2", "TRA2020_Erased", "RestofBA_PartialErased3", None)
    arcpy.analysis.Erase("RestofBA_PartialErased3", "TPA2020", "RestofBA_Erased", None)

    logger.info('Merge into 1 feature class with 5 polygons: Major Transit stop, high freq corridor, 15-30min bus, 31+min bus and remainder of region')
    arcpy.management.Merge("RestofBA_Erased;Bus30Plus_Erased;Bus15_30_Erased;TRA2020_Erased;TPA2020", "Transit2020")

    arcpy.management.Delete(["Bus15_30_PartialErased","Bus30Plus_PartialErased1",\
                            "Bus30Plus_PartialErased2","RestofBA_PartialErased1",\
                            "RestofBA_PartialErased2","RestofBA_PartialErased3",\
                            "RestofBA_Erased","Bus30Plus_Erased","Bus15_30_Erased","TRA2020_Erased"])

    ####repeat the same process for No Plan, projects that are in No Plan are automatically high quality transit as a major transit stop
    logger.info('Select and Buffer No Plan stops')
    arcpy.management.SelectLayerByAttribute("transit_potential", "NEW_SELECTION", "status = 'Under Construction'", None)
    arcpy.analysis.Buffer("transit_potential", "TPA2050_NP", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    logger.info('Merge and create a new TPA under NP')
    arcpy.management.Merge("TPA2050_NP; TPA2020", "TPA2050_NP_Merge")
    #dissolve so easier when merged
    arcpy.management.Dissolve("TPA2050_NP_Merge", "TPA2050_NP_Dislve", None, None, "MULTI_PART", "DISSOLVE_LINES")

    arcpy.analysis.Erase("TRA2020", "TPA2050_NP_Dislve", "TRA2050_NP", None)

    logger.info('Create Bus 15-30min shape for NP')
    arcpy.analysis.Erase("Bus15_30", "TRA2050_NP", "Bus15_30_2050NP_PartialErased", None)
    arcpy.analysis.Erase("Bus15_30_2050NP_PartialErased", "TPA2050_NP_Dislve", "Bus15_30_2050NP", None)

    logger.info('Create Bus 31+ min shape for NP')
    arcpy.analysis.Erase("Bus30Plus", "Bus15_30_2050NP", "Bus30Plus_2050NP_PartialErased1", None)
    arcpy.analysis.Erase("Bus30Plus_2050NP_PartialErased1", "TRA2050_NP", "Bus30Plus_2050NP_PartialErased2", None)
    arcpy.analysis.Erase("Bus30Plus_2050NP_PartialErased2", "TPA2050_NP_Dislve", "Bus30Plus_2050NP", None)

    logger.info('Create Rest of Bay Area Shape for NP')
    arcpy.analysis.Erase("BAcounty_expand", "Bus30Plus_2050NP", "RestofBA_2050NP_PartialErased1", None)
    arcpy.analysis.Erase("RestofBA_2050NP_PartialErased1", "Bus15_30_2050NP", "RestofBA_2050NP_PartialErased2", None)
    arcpy.analysis.Erase("RestofBA_2050NP_PartialErased2", "TRA2050_NP", "RestofBA_2050NP_PartialErased3", None)
    arcpy.analysis.Erase("RestofBA_2050NP_PartialErased3", "TPA2050_NP_Dislve", "RestofBA_2050NP", None)

    logger.info('Merge into 1 feature class with 5 polygons')
    arcpy.management.Merge("RestofBA_2050NP;Bus30Plus_2050NP; Bus15_30_2050NP;TRA2050_NP;TPA2050_NP_Dislve", "Transit2050_NP")

    arcpy.management.Delete(["Bus15_30_2050NP_PartialErased","Bus30Plus_2050NP_PartialErased1",\
                            "Bus30Plus_2050NP_PartialErased2","RestofBA_2050NP_PartialErased1",\
                            "RestofBA_2050NP_PartialErased2","RestofBA_2050NP_PartialErased3",\
                            "RestofBA_2050NP","Bus30Plus_2050NP","Bus15_30_2050NP","TRA2050_NP","TPA2050_NP_Merge","TPA2050_NP_Dislve"])

    ####repeat the same process for DBP Plan, projects that are in No Plan are automatically high quality transit as a major transit stop -- this to revise to Final Blue Print
    logger.info('Select and Buffer No Plan stops')
    arcpy.management.SelectLayerByAttribute("transit_potential", "NEW_SELECTION", "status = 'Under Construction' Or status = 'Draft Blueprint'", None)
    arcpy.analysis.Buffer("transit_potential", "TPA2050_BP", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    logger.info('Merge and create a new TPA under BP')
    arcpy.management.Merge("TPA2050_BP; TPA2020", "TPA2050_BP_Merge")
    #dissolve so easier when merged
    arcpy.management.Dissolve("TPA2050_BP_Merge", "TPA2050_BP_Dislve", None, None, "MULTI_PART", "DISSOLVE_LINES")

    arcpy.analysis.Erase("TRA2020", "TPA2050_BP_Dislve", "TRA2050_BP", None)

    logger.info('Create Bus 15-30min shape for BP')
    arcpy.analysis.Erase("Bus15_30", "TRA2050_BP", "Bus15_30_2050BP_PartialErased", None)
    arcpy.analysis.Erase("Bus15_30_2050BP_PartialErased", "TPA2050_BP_Dislve", "Bus15_30_2050BP", None)

    logger.info('Create Bus 31+ min shape for BP')
    arcpy.analysis.Erase("Bus30Plus", "Bus15_30_2050BP", "Bus30Plus_2050BP_PartialErased1", None)
    arcpy.analysis.Erase("Bus30Plus_2050BP_PartialErased1", "TRA2050_BP", "Bus30Plus_2050BP_PartialErased2", None)
    arcpy.analysis.Erase("Bus30Plus_2050BP_PartialErased2", "TPA2050_BP_Dislve", "Bus30Plus_2050BP", None)

    logger.info('Create Rest of Bay Area Shape for BP')
    arcpy.analysis.Erase("BAcounty_expand", "Bus30Plus_2050BP", "RestofBA_2050BP_PartialErased1", None)
    arcpy.analysis.Erase("RestofBA_2050BP_PartialErased1", "Bus15_30_2050BP", "RestofBA_2050BP_PartialErased2", None)
    arcpy.analysis.Erase("RestofBA_2050BP_PartialErased2", "TRA2050_BP", "RestofBA_2050BP_PartialErased3", None)
    arcpy.analysis.Erase("RestofBA_2050BP_PartialErased3", "TPA2050_BP_Dislve", "RestofBA_2050BP", None)

    logger.info('Merge into 1 feature class with 5 polygons')
    arcpy.management.Merge("RestofBA_2050BP;Bus30Plus_2050BP; Bus15_30_2050BP;TRA2050_BP;TPA2050_BP_Dislve", "Transit2050_BP")

    arcpy.management.Delete(["Bus15_30_2050BP_PartialErased","Bus30Plus_2050BP_PartialErased1",\
                            "Bus30Plus_2050BP_PartialErased2","RestofBA_2050BP_PartialErased1",\
                            "RestofBA_2050BP_PartialErased2","RestofBA_2050BP_PartialErased3",\
                            "RestofBA_2050BP","Bus30Plus_2050BP","Bus15_30_2050BP","TRA2050_BP","TPA2050_BP_Merge","TPA2050_BP_Dislve"])

    ###Bring in urbansim results
    acrossruns_proximity = []

    for us_runid in list_us_runid:
        urbansim_runid = urbansim_run_location + us_runid

        proximity = []
        # 2015 results first
        logger.info('Reading 2015 parcel data')
        parcel_output_2015 = pd.read_csv((urbansim_runid+'_parcel_data_2015.csv'))
        # keeping essential columns
        parcel_output_2015.drop(['geom_id','total_residential_units','total_job_spaces','zoned_du',\
                                'zoned_du_underbuild', 'zoned_du_underbuild_nodev', 'first_building_type'], axis=1, inplace=True)
        
        parcel_output_2015['totemp'] = parcel_output_2015['totemp'].fillna(0)
        parcel_output_2015['totemp'] = parcel_output_2015['totemp'].round(0).astype('int')
        parcel_output_2015['RETEMPN'] = parcel_output_2015['RETEMPN'].fillna(0)
        parcel_output_2015['RETEMPN'] = parcel_output_2015['RETEMPN'].round(0).astype('int')
        parcel_output_2015['MWTEMPN'] = parcel_output_2015['MWTEMPN'].fillna(0)
        parcel_output_2015['MWTEMPN'] = parcel_output_2015['MWTEMPN'].round(0).astype('int')
        
        parcel_summary_2015 = os.path.join(WORKING_DIR,'parcel_2015.csv')
    
        if arcpy.Exists(parcel_summary_2015):
            arcpy.management.Delete(parcel_summary_2015)
            parcel_output_2015.to_csv('parcel_2015.csv')
        else:
            parcel_output_2015.to_csv('parcel_2015.csv')

        arcpy.management.XYTableToPoint(parcel_summary_2015,'parcel_2015','x','y')

        sumFields = [['tothh', 'SUM'], ['hhq1', 'SUM'],['totemp', 'SUM'],['RETEMPN', 'SUM'],['MWTEMPN', 'SUM']]

        parcel_2015   = 'parcel_2015'
        transit_2020   = 'Transit2020'
        prox2015 = os.path.join(P2T_GDB, 'prox2015')

        logger.info('Summarizing 2015 parcel data')
        arcpy.analysis.SummarizeWithin(transit_2020, parcel_2015, prox2015, "KEEP_ALL", sumFields)
        
        prox2015_sdf = pd.DataFrame.spatial.from_featureclass(prox2015)

        prox2015_sdf['Service_Level']=['No Fixed Route Transit','Bus 31+min','Bus 15-30min','Bus <15min','Major Transit Stop']
        prox2015_sdf['tothh_share']=round(prox2015_sdf.SUM_tothh/parcel_output_2015.tothh.sum(),2)
        prox2015_sdf['hhq1_share']=round(prox2015_sdf.SUM_hhq1/parcel_output_2015.hhq1.sum(),2)
        prox2015_sdf['totemp_share']=round(prox2015_sdf.SUM_totemp/parcel_output_2015.totemp.sum(),2)
        prox2015_sdf['RETEMPN_share']=round(prox2015_sdf.SUM_RETEMPN/parcel_output_2015.RETEMPN.sum(),2)
        prox2015_sdf['MWTEMPN_share']=round(prox2015_sdf.SUM_MWTEMPN/parcel_output_2015.MWTEMPN.sum(),2)
        prox2015_sdf['year']='2015'
        prox2015_sdf['modelrunID'] = us_runid

        proximity.append(prox2015_sdf)

        arcpy.management.Delete(prox2015)
        arcpy.management.Delete(parcel_summary_2015)
            

        #2050 NP and BP
        logger.info('Reading 2050 parcel data')
        parcel_output_2050 = pd.read_csv((urbansim_runid+'_parcel_data_2050.csv'))
        parcel_output_2050.drop(['geom_id','total_residential_units','total_job_spaces','zoned_du',\
                                'zoned_du_underbuild', 'zoned_du_underbuild_nodev', 'first_building_type'], axis=1, inplace=True)

        parcel_output_2050['totemp'] = parcel_output_2050['totemp'].fillna(0)
        parcel_output_2050['totemp'] = parcel_output_2050['totemp'].round(0).astype('int')
        parcel_output_2050['RETEMPN'] = parcel_output_2050['RETEMPN'].fillna(0)
        parcel_output_2050['RETEMPN'] = parcel_output_2050['RETEMPN'].round(0).astype('int')
        parcel_output_2050['MWTEMPN'] = parcel_output_2050['MWTEMPN'].fillna(0)
        parcel_output_2050['MWTEMPN'] = parcel_output_2050['MWTEMPN'].round(0).astype('int')
        
        parcel_summary_2050 = os.path.join(WORKING_DIR,'parcel_2050.csv')

        if arcpy.Exists(parcel_summary_2050):
            arcpy.management.Delete(parcel_summary_2050)
            parcel_output_2050.to_csv('parcel_2050.csv')
        else:
            parcel_output_2050.to_csv('parcel_2050.csv')
    
        arcpy.management.XYTableToPoint(parcel_summary_2050,'parcel_2050','x','y')

        parcel_2050 = 'parcel_2050'
        transit_2050 = ['Transit2050_NP','Transit2050_BP']
    
        for transit in transit_2050:
            prox2050 = os.path.join(P2T_GDB, 'prox2050')
            logger.info('Summarizing {} parcel data'.format(transit))
            arcpy.analysis.SummarizeWithin(transit, parcel_2050, prox2050, "KEEP_ALL", sumFields)
            prox2050_sdf = pd.DataFrame.spatial.from_featureclass(prox2050)

            prox2050_sdf['Service_Level']=['No Fixed Route Transit','Bus 31+min','Bus 15-30min','Bus <15min','Major Transit Stop']
            prox2050_sdf['tothh_share']=round(prox2050_sdf.SUM_tothh/parcel_output_2050.tothh.sum(),2)
            prox2050_sdf['hhq1_share']=round(prox2050_sdf.SUM_hhq1/parcel_output_2050.hhq1.sum(),2)
            prox2050_sdf['totemp_share']=round(prox2050_sdf.SUM_totemp/parcel_output_2050.totemp.sum(),2)
            prox2050_sdf['RETEMPN_share']=round(prox2050_sdf.SUM_RETEMPN/parcel_output_2050.RETEMPN.sum(),2)
            prox2050_sdf['MWTEMPN_share']=round(prox2050_sdf.SUM_MWTEMPN/parcel_output_2050.MWTEMPN.sum(),2)
            prox2050_sdf['year']='2050'
            prox2050_sdf['modelrunID'] = us_runid + transit[-2:]

            proximity.append(prox2050_sdf)

            arcpy.management.Delete(prox2050)
        arcpy.management.Delete(parcel_summary_2050)


        proximity_parcel = pd.concat(proximity, ignore_index=True, sort=False)
        proximity_export = proximity_parcel[['modelrunID','year','Service_Level','SUM_tothh','tothh_share','SUM_hhq1','hhq1_share',\
                                        'SUM_totemp','totemp_share','SUM_RETEMPN','RETEMPN_share','SUM_MWTEMPN','MWTEMPN_share']]
        
        acrossruns_proximity.append(proximity_export)

    acrossruns_proximity_export = pd.concat(acrossruns_proximity, ignore_index=True, sort=False)
    acrossruns_proximity_export.to_csv('metrics_proximity_{}.csv'.format(NOW),index = False)