import os, sys, time, traceback
import arcpy
import logging
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import pandas as pd
import numpy as np

NOW = time.strftime("%Y_%m%d_%H%M")

# set PATH=C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3
###ArcGIS portion setup
if os.getenv("USERNAME")=="MTCPB":
    WORKING_DIR   = "C:\\Users\\MTCPB\\Documents\\ArcGIS\\Projects\\P2T"
else:
    WORKING_DIR   = os.path.abspath(".")

LOG_FILE          = os.path.join(WORKING_DIR,"proximity2transit_{}.log".format(NOW))
WORKSPACE_GDB     = "workspace_{}.GDB".format(NOW)

# To make these accessible, login to the server via the ArcGIS Pro GUI
MTC_ONLINE_TRANSIT_URL = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/DRAFT_transit_stops_planned_existing_01_2021/FeatureServer/\
0?token=qfm30yuQYooZDutSH8EvOs0Dm5v8ebszeE-B48VEw2HcmrNvB_X1AWZ--CP_jQzVI9eREVXfkN2XTqg\
IpSj2J8j12F3wMzajhTdIM3dAobQwbJE2kJg-DB06j6gD-yHqCyxhAynGAVCdAqtvMuoYV3NyBH7Ye_uowos7un\
hxKX2Bc3NNCPKcueIu4zmJrlZWE7UKU5wGuglc-vVxjxrg5pvn8zh_HgHcAfKj8XkZOJ4.'
MTC_ONLINE_BACOUNTY_URL  = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/county_region/FeatureServer/\
0?token=CjEwvAXFsqxLRQUDXcXdE2UiCvnfSdSnbwyLWY9nyEd3_X-8fqweg4FGuxjPXZ-GLsX8Hu5K_u0JfQP\
8VlapfgMcPFqFNzAkh7gjRLJnjO_a72KZtHGm3OSl-GLC3v4hqxpS478NRaSkQJB5fCLtvn_9nDiagBLbpldEwgL\
ruCJpsNDb9TPyyyrJ13Vl9LtZ-xZDqyZ5762Kju_a-nydWOqk0tB6ri9fPHh67sElVVA.'
MTC_ONLINE_TAZ_URL = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/taz_category/FeatureServer/\
0?token=BJaIzf_AGv0xs3qOCRsGq1QGDJRi-oT1T2HNUjjdRS_rtglT9HmP523amh626byf7YyYkkY0FvP6mcrc\
beLuttvpuc6mNL9K7HTFIUCaOEGLICEsqX4M3DahihVWZUfj1rVMmxI0CiECH6L_H0U-91bK3TevqBE2oOj4om0lw\
KhnKJVgE4yrf3y5GcTwe8yXMTZUZYMQSl-GxuKrbSYukYONX5Giyxn8NBx2kGGCf_4.'
MTC_ONLINE_TRACT_URL = 'https://arcgis.ad.mtc.ca.gov/server/rest/services/Hosted/tract_category/FeatureServer/\
0?token=5RNhXyoN6RmjOZmb9t4-uB-D0qqL1ZxZmcleQmVyif-5krodzSD-zaapplnxhORIhorr1szQ0099rbwkVe\
MsD-qZFF1bWqwvuQweifh4vBuBda6MBO3Ac9q-oEq4Wt6ESrtBuPi3ctPrk26cH4k_VBiwDJwhE4F4SczIOMyvNfwe\
jI9PglfGfZFw8N45yh71ywfAOoppATaxBDYQ7Ujv-hUS-DJZsi8voIpMg78RGpQ.'

###Urbansim Setup
urbansim_run_location           = 'C:/Users/{}/Box/Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/PBA50/'.format(os.getenv('USERNAME'))
us_2050_DBP_Final         = 'Draft Blueprint runs/Blueprint Plus Crossing (s23)/v1.7.1- FINAL DRAFT BLUEPRINT/run98'
us_2050_FBP_Final      = 'Final Blueprint runs/Final Blueprint (s24)/BAUS v2.25 - FINAL VERSION/run182'

list_us_runid = [us_2050_DBP_Final]#,us_2050_FBP_Final] 


def log_workspace_contents(logger):

    workspaces = arcpy.ListWorkspaces()
    for workspace in workspaces:
        logger.info("Workspace: {}".format(workspace))

    logger.info("arcpy.env.workspace: {}".format(arcpy.env.workspace))
  
    for dataset in arcpy.ListDatasets():
        logger.info("  dataset: {}".format(dataset))
        logger.info("    feature classes: {} ".format(arcpy.ListFeatureClasses(feature_dataset=dataset)))
    
    logger.info("  feature classes: {} ".format(arcpy.ListFeatureClasses()))
    logger.info("  tables: {} ".format(arcpy.ListTables()))

def run_as_external_process(python_str):
    """
    Run this as an external python process
    This isn't used currently; it was added to test if it would resolve the SummarizeWithin_analysis() error
    """
    import subprocess
    try:
        completed = subprocess.run(['python', '-c', python_str], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as err:
        print('ERROR:', err)
    else:
        print('returncode:', completed.returncode)
        print('Have {} bytes in stdout: {!r}'.format(len(completed.stdout), completed.stdout.decode('utf-8')))
        print('Have {} bytes in stderr: {!r}'.format(len(completed.stderr), completed.stderr.decode('utf-8')))

def create_transit_features(logger, transit_type):
    """
    transit_type is one of [current, noplan, blueprint]
    """
    logger.info("==== create_transit_features({}) ====".format(transit_type))
    if transit_type == "current":
        input_layer = "transit_current"
        prefix      = "trn_cur"
    elif transit_type == "noplan":
        input_layer = "transit_potential"
        prefix      = "trn_np"
        curprefix   = "trn_cur"
    elif transit_type == "blueprint":
        input_layer = "transit_potential"
        prefix      = "trn_bp"
        curprefix   = "trn_cur"
    else:
        logger.fatal("Unsupported transit_type {}".format(transit_type))

    orig_result = arcpy.GetCount_management(input_layer)
    logger.info("{} has {} rows".format(input_layer, orig_result[0]))

    ### buffered transit_current major stops
    if transit_type == "current":
        logger.info('Select buffer for major transit stops => {}_majorbuf'.format(prefix))
        major = arcpy.SelectLayerByAttribute_management(input_layer, "NEW_SELECTION", '"major_stop" = 1')
        arcpy.CopyFeatures_management(major, prefix+"_major")  # save selection to new feature class
        major_result = arcpy.GetCount_management(prefix+"_major")
        logger.info("  {}_major has {} rows".format(prefix, major_result[0]))
        arcpy.Buffer_analysis(prefix+"_major", prefix+"_majorbuf", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")
   
    else:
        if transit_type=="noplan":
            logger.info('Selecting "Under Construction" or "Open" stops for no plan')
            new_major = arcpy.management.SelectLayerByAttribute(input_layer, "NEW_SELECTION", 
                                                                '"status" = \'Under Construction\' Or status=\'Open\'')
        elif transit_type=="blueprint":
            logger.info('Selecting "Under Construction" or "Open" or "Final Blueprint" stops for no plan')
            new_major = arcpy.management.SelectLayerByAttribute(input_layer, "NEW_SELECTION", 
                                                                '"status" = \'Under Construction\' Or status=\'Open\' Or status=\'Final Blueprint\'')

        arcpy.CopyFeatures_management(new_major, prefix+"_new_major")  # save selection to new feature class
        new_major_result = arcpy.GetCount_management(prefix+"_new_major")
        logger.info("  {}_new_major has {} rows".format(prefix, new_major_result[0]))
        arcpy.Buffer_analysis(prefix+"_new_major", prefix+"_newmajorbuf", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

        # merge new major buffered with original major buffered
        arcpy.Merge_management([curprefix+"_majorbuf",prefix+"_newmajorbuf"], prefix+"_majorbuf_predissolve")
        # dissolve
        arcpy.management.Dissolve(prefix+"_majorbuf_predissolve", prefix+"_majorbuf", 
                                  dissolve_field=None, statistics_fields=None, multi_part="MULTI_PART", unsplit_lines="DISSOLVE_LINES")
    
    ### buffered hdway_15min stops
    logger.info('Creating buffer for stops with headway < 15min => {}_hdwy15buf'.format(prefix))

    if transit_type=="current":
        hdwy15 = arcpy.SelectLayerByAttribute_management(input_layer, "NEW_SELECTION", "am_av_hdwy <= 15 And pm_av_hdwy <= 15") 
        arcpy.CopyFeatures_management(hdwy15, prefix+"_hdwy15")  # save selection to new feature class
        hdwy15_result = arcpy.GetCount_management(prefix+"_hdwy15")
        logger.info("  {}_hdwy15 has {} rows".format(prefix, hdwy15_result[0]))
        arcpy.Buffer_analysis(prefix+"_hdwy15", prefix+"_hdwy15buf", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    else:
        # copy current
        arcpy.CopyFeatures_management(curprefix+"_hdwy15buf", prefix+"_hdwy15buf")

    ### buffered hdway_30min stops
    logger.info('Creating buffer for stops with headway 15-30 min => {}_hdwy30buf'.format(prefix))

    if transit_type=="current":
        hdwy30 = arcpy.SelectLayerByAttribute_management(input_layer, "NEW_SELECTION", "am_av_hdwy > 15 And am_av_hdwy <= 30 And pm_av_hdwy > 15 And pm_av_hdwy <= 30")
        arcpy.CopyFeatures_management(hdwy30, prefix+"_hdwy30")  # save selection to new feature class
        hdwy30_result = arcpy.GetCount_management(prefix+"_hdwy30")
        logger.info("  {}_hdwy30 has {} rows".format(prefix, hdwy30_result[0]))
        arcpy.Buffer_analysis(prefix+"_hdwy30", prefix+"_hdwy30buf", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    else:
        # copy current
        arcpy.CopyFeatures_management(curprefix+"_hdwy30buf", prefix+"_hdwy30buf") 

    ### buffered hdway_30plus stops
    logger.info('Creating buffer for stops with headway 30+ min => {}_hdwy30plusbuf'.format(prefix))

    if transit_type=="current":
        hdwy30plus = arcpy.SelectLayerByAttribute_management(input_layer, "NEW_SELECTION", "am_av_hdwy > 30 Or pm_av_hdwy > 30")
        arcpy.CopyFeatures_management(hdwy30plus, prefix+"_hdwy30plus")  # save selection to new feature class
        hdwy30plus_result = arcpy.GetCount_management(prefix+"_hdwy30plus")
        logger.info("  {}_hdwy30plus has {} rows".format(prefix, hdwy30plus_result[0]))
        arcpy.Buffer_analysis(prefix+"_hdwy30plus", prefix+"_hdwy30plusbuf", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")

    else:
        # copy current
        arcpy.CopyFeatures_management(curprefix+"_hdwy30plusbuf", prefix+"_hdwy30plusbuf") 

    ### Make them disjoint -- first one wins
    logger.info('Isolate {}_hdwy15buf => {}_hdwy15buf_only'.format(prefix,prefix))
    arcpy.Erase_analysis(in_features=prefix+"_hdwy15buf", erase_features=prefix+"_majorbuf",
                         out_feature_class=prefix+"_hdwy15buf_only")

    logger.info('Isolate {}_hdwy30buf => {}_hdwy30buf_only'.format(prefix,prefix))
    arcpy.Erase_analysis(prefix+"_hdwy30buf",   prefix+"_majorbuf",  prefix+"_hdwy30buf_1")
    arcpy.Erase_analysis(prefix+"_hdwy30buf_1", prefix+"_hdwy15buf", prefix+"_hdwy30buf_only")
    arcpy.Delete_management([prefix+"_hdwy30buf_1"])

    logger.info('Isolate {}_hdwy30plusbuf  => {}_hdwy30plusbuf_only'.format(prefix,prefix))
    arcpy.Erase_analysis(prefix+"_hdwy30plusbuf",   prefix+"_majorbuf",  prefix+"_hdwy30plusbuf_1")
    arcpy.Erase_analysis(prefix+"_hdwy30plusbuf_1", prefix+"_hdwy15buf", prefix+"_hdwy30plusbuf_2")
    arcpy.Erase_analysis(prefix+"_hdwy30plusbuf_2", prefix+"_hdwy30buf", prefix+"_hdwy30plusbuf_only")
    arcpy.Delete_management([prefix+"_hdwy30plusbuf_1", 
                             prefix+"_hdwy30plusbuf_2"])

    logger.info('Rest of Bay Area => {}_none'.format(prefix))
    arcpy.Erase_analysis("BAcounty_expand",   prefix+"_majorbuf",      "BAcounty_expand_1")
    arcpy.Erase_analysis("BAcounty_expand_1", prefix+"_hdwy15buf",     "BAcounty_expand_2")
    arcpy.Erase_analysis("BAcounty_expand_2", prefix+"_hdwy30buf",     "BAcounty_expand_3")
    arcpy.Erase_analysis("BAcounty_expand_3", prefix+"_hdwy30plusbuf", prefix+"_none")
    arcpy.Delete_management(["BAcounty_expand_1",
                             "BAcounty_expand_2",
                             "BAcounty_expand_3"])


    logger.info('Merge into one feature class => {}_cat5'.format(prefix))
    arcpy.Merge_management([prefix+"_none",
                            prefix+"_hdwy30plusbuf_only",
                            prefix+"_hdwy30buf_only",
                            prefix+"_hdwy15buf_only",
                            prefix+"_majorbuf"],
                            prefix+"_cat5", add_source="ADD_SOURCE_INFO")
    # create Service_Level from MERGE_SRC
    arcpy.AddField_management(prefix +"_cat5", "Service_Level", "TEXT","","", 200)
    with arcpy.da.UpdateCursor(prefix +"_cat5", ["Service_Level", "MERGE_SRC"]) as cursor:
            for row in cursor:
                if 'none' in row[1]:
                    row[0] = 'No_Fixed_Route_Transit'
                elif 'hdwy30plusbuf' in row[1]:
                    row[0] = 'Bus_31plus_min'
                elif 'hdwy30buf' in row[1]:
                    row[0] = 'Bus_15_30min'
                elif 'hdwy15buf' in row[1]:
                    row[0] = 'Bus_<15min'
                elif 'majorbuf' in row[1]:
                    row[0] = 'Major_Transit_Stop'
                cursor.updateRow(row) 

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

    arcpy.CreateFileGDB_management(WORKING_DIR, WORKSPACE_GDB)
    arcpy.env.workspace = os.path.join(WORKING_DIR,WORKSPACE_GDB)
    arcpy.env.overwriteOutput = True

    transit_layer            = arcpy.MakeFeatureLayer_management(MTC_ONLINE_TRANSIT_URL,'transit_layer')
    bacounty_portal          = arcpy.MakeFeatureLayer_management(MTC_ONLINE_BACOUNTY_URL,'bacounty_portal')
    taz_portal               = arcpy.MakeFeatureLayer_management(MTC_ONLINE_TAZ_URL,'taz_portal')
    tract_portal             = arcpy.MakeFeatureLayer_management(MTC_ONLINE_TRACT_URL,'tract_portal')

    bacounty                 = os.path.join(arcpy.env.workspace, "bacounty")
    taz                      = os.path.join(arcpy.env.workspace, "taz")
    tract                    = os.path.join(arcpy.env.workspace, "tract")

    if arcpy.Exists(bacounty):          arcpy.Delete_management(bacounty)
    if arcpy.Exists(taz):               arcpy.Delete_management(taz)
    if arcpy.Exists(tract):             arcpy.Delete_management(tract)
    arcpy.FeatureClassToFeatureClass_conversion(bacounty_portal,          arcpy.env.workspace,'bacounty')
    arcpy.FeatureClassToFeatureClass_conversion(taz_portal,               arcpy.env.workspace,'taz')
    arcpy.FeatureClassToFeatureClass_conversion(tract_portal,             arcpy.env.workspace,'tract')

    transit_current = arcpy.SelectLayerByAttribute_management(transit_layer, "NEW_SELECTION", "status = 'Existing/Built'")
    arcpy.CopyFeatures_management(transit_current, 'transit_current')     

    transit_potential = arcpy.SelectLayerByAttribute_management(transit_layer, "NEW_SELECTION", "status <> 'Existing/Built'")
    arcpy.CopyFeatures_management(transit_potential, 'transit_potential')

    # log info about the workspace
    log_workspace_contents(logger)

    #Using county boundaries alone did not capture all parcels (1,952,484/1,956,212). With buffering 0.5 mi 1,956,044/1,956,212 99.99% of all parcels
    logger.info('Create buffered bacounty => bacounty_expand')
    arcpy.analysis.Buffer("bacounty", "bacounty_expand", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


    # create trncur_cat layer, which has 5 disjoint features which together make bacounty_exapdn: 
    # majorbuf, hdwy15buf_only hdwy30buf_only, hdwy30plusbuf_only, none
    create_transit_features(logger, "current")

    # create same 5 layers for no plan, which is the same as above but includes transit_potential status='Under Construction' as major stops as well
    create_transit_features(logger, "noplan")

    # create same 5 layers for blueprint, which is the same as above but includes transit_potential status='Draft Blueprint' as major stops as well
    create_transit_features(logger, "blueprint")

    ### Bring in urbansim results
    all_prox = pd.DataFrame()

    for us_runid in list_us_runid:
        logger.info("")
        logger.info("==== Processing UrbanSim run {} ====".format(us_runid))
        urbansim_runid = os.path.join(urbansim_run_location,us_runid)

        for model_year in (2015, 2050):
            if model_year == 2050:
                if us_runid == us_2050_FBP_Final:
                    parcel_file = urbansim_runid+'_parcel_data_{}_UBI.csv'.format(model_year)
            else: 
                parcel_file = urbansim_runid+'_parcel_data_{}.csv'.format(model_year)
            logger.info('Reading {} parcel data from {}'.format(model_year, parcel_file))
            parcel_output = pd.read_csv(parcel_file, engine='python' )

            logger.info('  Read {} rows'.format(len(parcel_output)))

            # keep essential columns
            parcel_output.drop(['geom_id','total_job_spaces','zoned_du',
                                'zoned_du_underbuild', 'zoned_du_underbuild_nodev', 'first_building_type'], axis=1, inplace=True)
            logger.info("Head:\n{}".format(parcel_output.head()))

            parcel_output['totemp' ] = parcel_output['totemp' ].fillna(0)
            parcel_output['totemp' ] = parcel_output['totemp' ].round(0).astype('int')
            parcel_output['RETEMPN'] = parcel_output['RETEMPN'].fillna(0)
            parcel_output['RETEMPN'] = parcel_output['RETEMPN'].round(0).astype('int')
            parcel_output['MWTEMPN'] = parcel_output['MWTEMPN'].fillna(0)
            parcel_output['MWTEMPN'] = parcel_output['MWTEMPN'].round(0).astype('int')
        
            # save as table in gdb
            parcel_table = os.path.join(arcpy.env.workspace, "parcel_table")
            if arcpy.Exists(parcel_table): arcpy.management.Delete(parcel_table)

            parcel_array = np.array(np.rec.fromrecords(parcel_output.values))
            parcel_array.dtype.names = tuple(parcel_output.dtypes.index.tolist())
            arcpy.da.NumPyArrayToTable(parcel_array, parcel_table)
            logger.info("Saved to {} in {}".format(parcel_table, arcpy.env.workspace))

            # convert to point feature class in GDB
            if arcpy.Exists('parcel_fc'): arcpy.management.Delete('parcel_fc')
            arcpy.management.XYTableToPoint(in_table=parcel_table, out_feature_class='parcel_fc',x_field='x',y_field='y')
            logger.info("Saved to {} in {}".format('parcel_fc', arcpy.env.workspace))

            if model_year == 2015:
                # current
                transit_features = ['trn_cur_cat5']
            elif model_year == 2050:
                # no plan and blueprint
                transit_features = ['trn_np_cat5', 'trn_fp_cat5']

            for transit_feature in transit_features:

                logger.info('Summarizing {} parcel data proximity to {}'.format(model_year, transit_feature))
                log_workspace_contents(logger)

                try:
                    logger.info("feature classes no paths")
                    arcpy.SummarizeWithin_analysis(transit_feature, 'parcel_fc', 'prox', keep_all_polygons="KEEP_ALL", 
                                                    sum_fields=[['tothh','SUM'], ['hhq1','SUM'],['totemp','SUM'],['RETEMPN','SUM'],['MWTEMPN','SUM']])
                    # hasn't worked, see comments below
                    logger.info("SUCCESS")
                except:
                    # Get the tool error messages 
                    msgs = arcpy.GetMessages(2)
                    logger.error("Exception occured; msgs: {}".format(msgs))

                    # Get the traceback object
                    tb = sys.exc_info()[2]
                    tbinfo = traceback.format_tb(tb)[0]

                    # Concatenate information together concerning the error into a message string
                    logger.error("Traceback info:\n{}\nError Info:\n{}".format(tbinfo, str(sys.exc_info()[1])))
                    logger.error("It's ok though -- we'll do this another way, but still trying the easy way")

                # Something related to arcpy.SummarizeWithin_analysis() is buggy
                # The following attempts have failed with 
                # ERROR 000187: Only supports Geodatabase tables and feature classes
                # * use the method with feature layers as inputs, with full paths and without
                # * use the method with feature classes as inputs, with full paths and without
                # * call the short script below via subprocess
                # * copy feature classes to arcpy.env.scratchGDB and summarizeWithin there
                #
                # HOWEVER, after this script fails, running the following on the command line succeeds:
                # 
                # >>> import arcpy
                # >>> arcpy.env.workspace='M:\Data\GIS layers\JobsHousingTransitProximity\workspace_2020_1007_1737.gdb'
                # >>> arcpy.SummarizeWithin_analysis('trn_cur_cat5', 'parcel_fc', 'prox', keep_all_polygons='KEEP_ALL', sum_fields=[['tothh','SUM'], ['hhq1','SUM'],['totemp','SUM'],['RETEMPN','SUM'],['MWTEMPN','SUM']])
                # <Result 'M:\\Data\\GIS layers\\JobsHousingTransitProximity\\workspace_2020_1007_1737.gdb\\prox'>
                # >>> prox_sdf = pd.DataFrame.spatial.from_featureclass('prox')
                #
                # so we'll summarize within ourselves and use spatial join instead
                if arcpy.Exists('parcel_fc_join_trn'): arcpy.management.Delete('parcel_fc_join_trn')
                logger.info("spatial joining parcel_fc with {}".format(transit_feature))
                arcpy.SpatialJoin_analysis(target_features='parcel_fc', join_features=transit_feature, 
                                           out_feature_class='parcel_fc_join_trn')

                logger.info("spatial joining parcel_fc_join_trn with {}".format(taz))
                arcpy.SpatialJoin_analysis(target_features='parcel_fc_join_trn', join_features=taz, 
                                           out_feature_class='parcel_fc_join_trn_taz')

                logger.info("spatial joining parcel_fc_join_trn_taz with {}".format(tract))
                arcpy.SpatialJoin_analysis(target_features='parcel_fc_join_trn_taz', join_features=tract, 
                                           out_feature_class='parcel_fc_join_trn_taz_tract')
                logger.info("    ...complete")

                prox_sdf = pd.DataFrame.spatial.from_featureclass('parcel_fc_join_trn_taz_tract')
                prox_sdf = prox_sdf.groupby('area_type','Service_Level').agg({'tothh':'sum', 'hhq1':'sum', 
                                                                 'totemp':'sum', 'RETEMPN':'sum',
                                                                 'MWTEMPN':'sum'}).reset_index()

                prox_sdf['tothh_share'  ] = round(prox_sdf.tothh  /prox_sdf.tothh.sum(),  2)
                prox_sdf['hhq1_share'   ] = round(prox_sdf.hhq1   /prox_sdf.hhq1.sum()  , 2)
                prox_sdf['totemp_share' ] = round(prox_sdf.totemp /prox_sdf.totemp.sum(), 2)
                prox_sdf['RETEMPN_share'] = round(prox_sdf.RETEMPN/prox_sdf.RETEMPN.sum(),2)
                prox_sdf['MWTEMPN_share'] = round(prox_sdf.MWTEMPN/prox_sdf.MWTEMPN.sum(),2)
                prox_sdf['year'         ] = str(model_year)
                prox_sdf['modelrunID'   ] = us_runid
                prox_sdf['transit'      ] = transit_feature
                prox_sdf['taz_cat'      ] = taz

                logger.info("prox_sdf:\n{}".format(prox_sdf))
                all_prox = all_prox.append(prox_sdf)

                logger.info("all_prox:\n{}".format(all_prox))

    # write it
    outfile = 'metrics_proximity_{}.csv'.format(NOW)
    logger.info("")
    all_prox.to_csv('metrics_proximity_{}.csv'.format(NOW), index=False)
    logger.info("Wrote {}".format(outfile))

