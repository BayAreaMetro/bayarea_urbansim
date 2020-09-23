
# coding: utf-8

# # Share of housing units near transit
# This is assessed for 2015 and 2050
# Proximity is defined in relation to "transit rich areas" which is defined as within .5 miles of a 'major transit stop' or .25 miles of a stop along a high quality bus corridor. A 'major transit stop' is defined by the state in Section 21064.3:
# 
# An existing rail transit station, a ferry terminal served by either a bus or rail transit service, or the intersection of two or more major bus routes with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods.
# 
# A high quality bus corridor is a corridor with fixed route bus service meeting specific criteria such as average service intervals of 15 minutes or less during peak weekday commute hours. 
# 

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import arcpy
import arcgis
from IPython.display import display
from arcgis import *
from arcgis.features import GeoAccessor, GeoSeriesAccessor


# In[2]:


gis=GIS("pro")


# ## Create Current Transit Layer

# In[ ]:


#Select buffer for major transit stops
arcpy.management.SelectLayerByAttribute("transitstops_012020", "NEW_SELECTION", "major_stop = 1", None)
arcpy.analysis.Buffer("transitstops_012020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TPA2020", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[ ]:


#Select buffer for headway<15min (TRA)
arcpy.management.SelectLayerByAttribute("transitstops_012020", "NEW_SELECTION", "hdwy_15min = 1", None)
arcpy.analysis.Buffer("transitstops_012020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TRA2020", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[ ]:


#Select buffer for headway 15-30 min
arcpy.management.SelectLayerByAttribute("transitstops_012020", "NEW_SELECTION", "hdwy_30min = 1", None)
arcpy.analysis.Buffer("transitstops_012020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[3]:


#Select buffer for headway 30+ min
arcpy.management.SelectLayerByAttribute("transitstops_012020", "NEW_SELECTION", "hdwy_class = '31 mins or more'", None)
arcpy.analysis.Buffer("transitstops_012020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus", "0.25 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[ ]:


#Using county boundaries alone did not capture all parcels (1,952,484/1,956,212). With buffering 0.5 mi 1,956,044/1,956,212 99.99% of all parcels
arcpy.analysis.Buffer("BAcounty", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\BAcounty_expand", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[ ]:


#only include high-freq bus area not already part of Major Transit Stop buffer
arcpy.analysis.Erase("TRA2020", "TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TRA2020_Erased", None)


# In[ ]:


#Create Bus 15-30min shape
arcpy.analysis.Erase("Bus15_30", "TRA2020_Erased", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30_PartialErased", None)
arcpy.analysis.Erase("Bus15_30_PartialErased", "TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30_Erased", None)


# In[4]:


#Create Bus 31+ min shape
arcpy.analysis.Erase("Bus30Plus", "Bus15_30_Erased", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_PartialErased1", None)
arcpy.analysis.Erase("Bus30Plus_PartialErased1", "TRA2020_Erased", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_PartialErased2", None)
arcpy.analysis.Erase("Bus30Plus_PartialErased2", "TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_Erased", None)


# In[5]:


#Create 'Rest of Bay Area' Shape
arcpy.analysis.Erase("BAcounty_expand", "Bus30Plus_Erased", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_PartialErased1", None)
arcpy.analysis.Erase("RestofBA_PartialErased1", "Bus15_30_Erased", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_PartialErased2", None)
arcpy.analysis.Erase("RestofBA_PartialErased2", "TRA2020_Erased", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_PartialErased3", None)
arcpy.analysis.Erase("RestofBA_PartialErased3", "TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_Erased", None)


# In[6]:


#Merge into 1 feature class with 5 polygons: Major Transit stop, high freq corridor, 15-30min bus, 31+min bus and remainder of region
#Note names in the new column were manually added
arcpy.management.Merge("RestofBA_Erased;Bus30Plus_Erased;Bus15_30_Erased;TRA2020_Erased;TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Transit2020", 'Shape_Length "Shape_Length" false true true 8 Double 0 0,First,#,RestofBA_Erased,Shape_Length,-1,-1,Bus30Plus_Erased,Shape_Length,-1,-1,Bus15_30_Erased,Shape_Length,-1,-1,TRA2020_Erased,Shape_Length,-1,-1,TPA2020,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0,First,#,RestofBA_Erased,Shape_Area,-1,-1,Bus30Plus_Erased,Shape_Area,-1,-1,Bus15_30_Erased,Shape_Area,-1,-1,TRA2020_Erased,Shape_Area,-1,-1,TPA2020,Shape_Area,-1,-1', "NO_SOURCE_INFO")


# ## Repeat Transit layer for 2050 (both NP and BP)
# Note lacking Milpitas BART stop (should be under Committed/Under Construction)
# <br>All potential projects are major transit stops
# <br>NP = no plan, BP = blueprint

# In[ ]:


#No Plan stops buffer
arcpy.management.SelectLayerByAttribute("transitstops_potential", "NEW_SELECTION", "status = 'Under Construction'", None)
arcpy.analysis.Buffer("transitstops_potential", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\New_TPA2050_NP", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[ ]:


#Draft Blueprint project buffer
arcpy.management.SelectLayerByAttribute("transitstops_potential", "NEW_SELECTION", "status = 'Under Construction' Or status = 'Draft Blueprint'", None)
arcpy.analysis.Buffer("transitstops_potential", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\NEW_TPA2050_BP", "0.5 Miles", "FULL", "ROUND", "ALL", None, "PLANAR")


# In[ ]:


#Combine each with existing TPA buffer for TPA2050_NP and TPA2050_BP
arcpy.management.Merge("TPA2050_NP;TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TPA2050_NP_Merge", 'FID_New_TPA2050_NP "FID_New_TPA2050_NP" true true false 4 Long 0 0,First,#,TPA2050_NP,FID_New_TPA2050_NP,-1,-1;FID_TPA2020 "FID_TPA2020" true true false 4 Long 0 0,First,#,TPA2050_NP,FID_TPA2020,-1,-1;Shape_Length "Shape_Length" false true true 8 Double 0 0,First,#,TPA2050_NP,Shape_Length,-1,-1,TPA2020,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0,First,#,TPA2050_NP,Shape_Area,-1,-1,TPA2020,Shape_Area,-1,-1', "NO_SOURCE_INFO")
#dissolve so easier when merged
arcpy.management.Dissolve("TPA2050_NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TPA2050_NP_Dislve2", None, None, "MULTI_PART", "DISSOLVE_LINES")
arcpy.management.Merge("NEW_TPA2050_BP;TPA2020", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TPA2050_BP", 'Shape_Length "Shape_Length" false true true 8 Double 0 0,Sum,#,NEW_TPA2050_BP,Shape_Length,-1,-1,TPA2020,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0,Sum,#,NEW_TPA2050_BP,Shape_Area,-1,-1,TPA2020,Shape_Area,-1,-1', "NO_SOURCE_INFO")
arcpy.management.Dissolve("TPA2050_BP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TPA2050_BP_Dislve", None, None, "MULTI_PART", "DISSOLVE_LINES")


# In[ ]:


#Erase activity again
arcpy.analysis.Erase("TRA2020", "TPA2050_BP_Dislve", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TRA2050_BP", None)
arcpy.analysis.Erase("TRA2020", "TPA2050_NP_Dislve2", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\TRA2050_NP", None)
#Bus 15-30 min
arcpy.analysis.Erase("Bus15_30", "TRA2050_NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30_2050NP_PartialErase", None)
arcpy.analysis.Erase("Bus15_30", "TRA2050_BP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30_2050BP_PartialErase", None)
arcpy.analysis.Erase("Bus15_30_2050BP_PartialErase", "TPA2050_BP_Dislve", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30_2050BP", None)
arcpy.analysis.Erase("Bus15_30_2050NP_PartialErase", "TPA2050_NP_Dislve2", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus15_30_2050NP", None)


# In[15]:


#Bus 31+ min
arcpy.analysis.Erase('Bus30Plus', 'Bus15_30_2050BP', r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_2050BP_PartialErased1", None)
arcpy.analysis.Erase("Bus30Plus_2050BP_PartialErased1", "TRA2050_BP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_2050BP_PartialErased2", None)
arcpy.analysis.Erase("Bus30Plus_2050BP_PartialErased2", "TPA2050_BP_Dislve", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_2050BP", None)
arcpy.analysis.Erase("Bus30Plus", "Bus15_30_2050NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_2050NP_PartialErased1", None)
arcpy.analysis.Erase("Bus30Plus_2050NP_PartialErased1", "TRA2050_NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_2050NP_PartialErased2", None)
arcpy.analysis.Erase("Bus30Plus_2050NP_PartialErased2", "TPA2050_NP_Dislve2", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Bus30Plus_2050NP", None)

#No fixed route transit (remainder of region)
arcpy.analysis.Erase("BAcounty_expand", "Bus30Plus_2050BP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050BP_PartialErased1", None)
arcpy.analysis.Erase("RestofBA_2050BP_PartialErased1", "Bus15_30_2050BP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050BP_PartialErased2", None)
arcpy.analysis.Erase("RestofBA_2050BP_PartialErased2", "TRA2050_BP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050BP_PartialErased3", None)
arcpy.analysis.Erase("RestofBA_2050BP_PartialErased3", "TPA2050_BP_Dislve", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050BP", None)
arcpy.analysis.Erase("BAcounty_expand", "Bus30Plus_2050NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050NP_PartialErased1", None)
arcpy.analysis.Erase("RestofBA_2050NP_PartialErased1", "Bus15_30_2050NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050NP_PartialErased2", None)
arcpy.analysis.Erase("RestofBA_2050NP_PartialErased2", "TRA2050_NP", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050NP_PartialErased3", None)
arcpy.analysis.Erase("RestofBA_2050NP_PartialErased3", "TPA2050_NP_Dislve2", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\RestofBA_2050NP", None)


# In[21]:


#Merge into Transit 2050 NP and BP 
arcpy.management.Merge("RestofBA_2050NP;Bus30Plus_2050NP;Bus15_30_2050NP;TRA2050_NP;TPA2050_NP_Dislve2", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Transit2050_NP", 'Shape_Length "Shape_Length" false true true 8 Double 0 0,First,#,RestofBA_2050NP,Shape_Length,-1,-1,Bus30Plus_2050NP,Shape_Length,-1,-1,Bus15_30_2050NP,Shape_Length,-1,-1,TRA2050_NP,Shape_Length,-1,-1,TPA2050_NP_Dislve2,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0,First,#,RestofBA_2050NP,Shape_Area,-1,-1,Bus30Plus_2050NP,Shape_Area,-1,-1,Bus15_30_2050NP,Shape_Area,-1,-1,TRA2050_NP,Shape_Area,-1,-1,TPA2050_NP_Dislve2,Shape_Area,-1,-1', "NO_SOURCE_INFO")
arcpy.management.Merge("RestofBA_2050BP;Bus30Plus_2050BP;Bus15_30_2050BP;TRA2050_BP;TPA2050_BP_Dislve", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\Transit2050_BP", 'Shape_Length "Shape_Length" false true true 8 Double 0 0,First,#,RestofBA_2050BP,Shape_Length,-1,-1,Bus30Plus_2050BP,Shape_Length,-1,-1,Bus15_30_2050BP,Shape_Length,-1,-1,TRA2050_BP,Shape_Length,-1,-1,TPA2050_BP_Dislve,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0,First,#,RestofBA_2050BP,Shape_Area,-1,-1,Bus30Plus_2050BP,Shape_Area,-1,-1,Bus15_30_2050BP,Shape_Area,-1,-1,TRA2050_BP,Shape_Area,-1,-1,TPA2050_BP_Dislve,Shape_Area,-1,-1', "NO_SOURCE_INFO")


# In[ ]:


#Summarize Within for NP and BP
arcpy.analysis.SummarizeWithin("Transit2050_NP", "Parcels2050", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2050_NP", "KEEP_ALL", "residential_units Sum;job_spaces Sum", "ADD_SHAPE_SUM", '', None, "NO_MIN_MAJ", "NO_PERCENT", None)


# ## Previous work (ignore)

# In[41]:


#not necessary now but useful for later
#Added suffix twice accidentally
#def drop_suffix(self, suffix):
#    self.columns = self.columns.str.rstrip(suffix)
#    return self
#pd.core.frame.DataFrame.drop_suffix = drop_suffix


# In[50]:


#not necessary now but useful for later
#Drop suffix
#prox_15_sdf.drop_suffix('_2015')
#prox_50NP_sdf.drop_suffix('_2050NP')
#prox_50BP_sdf.drop_suffix('_2050BP')


# # Summarize Parcels in Transit 

# In[4]:


#Use summarize within of Parcels 2015 in Transit 2020
#Note WANT RETEMPN and MWTEMPN but because of run98 mix-up, collected 'OTHEMPN' for RETEMPN and 'FPSEMPN' for MWTEMPN
arcpy.analysis.SummarizeWithin("Transit2020", "Parcels2015", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2015", "KEEP_ALL", "tothh Sum;hhq1 Sum;hhq2 Sum;hhq3 Sum;hhq4 Sum;totemp Sum;RETEMPN Sum;MWTEMPN Sum", "NO_SHAPE_SUM", '', None, "NO_MIN_MAJ", "NO_PERCENT", None)


# In[5]:


#Parcels 2050 with Transit No Plan
arcpy.analysis.SummarizeWithin("Transit2050_NP", "Parcels2050", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2050_NP", "KEEP_ALL", "tothh Sum;hhq1 Sum;hhq2 Sum;hhq3 Sum;hhq4 Sum;totemp Sum;RETEMPN Sum;MWTEMPN Sum", "NO_SHAPE_SUM", '', None, "NO_MIN_MAJ", "NO_PERCENT", None)


# In[6]:


#Parcels 2050 with Transit Blueprint
arcpy.analysis.SummarizeWithin("Transit2050_BP", "Parcels2050", r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2050_BP", "KEEP_ALL", "tothh Sum;hhq1 Sum;hhq2 Sum;hhq3 Sum;hhq4 Sum;totemp Sum;RETEMPN Sum;MWTEMPN Sum", "NO_SHAPE_SUM", '', None, "NO_MIN_MAJ", "NO_PERCENT", None)


# In[70]:


#Convert to sdfs
Pprox2015 = r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2015"
Pprox_15_sdf = pd.DataFrame.spatial.from_featureclass(Pprox2015)
par_15=pd.read_csv(r"C:\Users\jhalpern\Desktop\C1-3\run98_parcel_data_2015.csv")


# In[4]:


#Convert to sdfs - MWT
Pprox2015 = r"C:\Users\jhalpern\Box\Horizon and Plan Bay Area 2050\Equity and Performance\7_Analysis\Metrics\Connected\Proximity2Transit\Proximity2Transit.gdb\PMWT_Proximity2015"
Pprox_15_sdf = pd.DataFrame.spatial.from_featureclass(Pprox2015)
par_15=pd.read_csv(r"C:\Users\jhalpern\Desktop\C1-3\run98_parcel_data_2015.csv")


# In[5]:


#Add service level name
Pprox_15_sdf['Service_Level']=['No Fixed Route Transit','Bus 31+min','Bus 15-30min','Bus <15min','Major Transit Stop']


# In[72]:


#SWITCH industries to correct run98
#do not apply to future runs
Pprox_15_sdf.rename(columns={'SUM_AGREMPN':'AGREMPN',
            'SUM_MWTEMPN':'FPSEMPN',
            'SUM_RETEMPN':'HEREMPN',
            'SUM_FPSEMPN':'MWTEMPN',
            'SUM_HEREMPN':'OTHEMPN',
            'SUM_OTHEMPN':'RETEMPN'}, inplace=True)


# In[6]:


Pprox_15_sdf


# In[74]:


#Calculate Shares
Pprox_15_sdf['tothh_share']=Pprox_15_sdf.SUM_tothh/par_15.tothh.sum()
Pprox_15_sdf['hhq1_share']=Pprox_15_sdf.SUM_hhq1/par_15.hhq1.sum()
Pprox_15_sdf['hhq2_share']=Pprox_15_sdf.SUM_hhq2/par_15.hhq2.sum()
Pprox_15_sdf['hhq3_share']=Pprox_15_sdf.SUM_hhq3/par_15.hhq3.sum()
Pprox_15_sdf['hhq4_share']=Pprox_15_sdf.SUM_hhq4/par_15.hhq4.sum()
Pprox_15_sdf['totemp_share']=Pprox_15_sdf.SUM_totemp/par_15.totemp.sum()
Pprox_15_sdf['RETEMPN_share']=Pprox_15_sdf.RETEMPN/par_15.RETEMPN.sum()
Pprox_15_sdf['MWTEMPN_share']=Pprox_15_sdf.MWTEMPN/par_15.MWTEMPN.sum()
Pprox_15_sdf['Scenario']='2015'


# In[7]:


#Calculate Shares - just MWT
Pprox_15_sdf['MWTEMPN_share']=Pprox_15_sdf.SUM_FPSEMPN2	/par_15.FPSEMPN.sum()
Pprox_15_sdf['Scenario']='2015'


# In[8]:


Pprox_15_sdf


# In[10]:


#Convert to sdfs-MWT
Pprox2050 = r"C:\Users\jhalpern\Box\Horizon and Plan Bay Area 2050\Equity and Performance\7_Analysis\Metrics\Connected\Proximity2Transit\Proximity2Transit.gdb\PMWT_Proximity2050"
Pprox_50BP_sdf = pd.DataFrame.spatial.from_featureclass(Pprox2050)
par_50=pd.read_csv(r"C:\Users\jhalpern\Desktop\C1-3\run98_parcel_data_2050.csv")


# In[42]:


#Convert to sdfs
Pprox2050BP = r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2050_BP"
Pprox_50BP_sdf = pd.DataFrame.spatial.from_featureclass(Pprox2050BP)
Pprox2050NP = r"C:\Users\jhalpern\Documents\ArcGIS\Projects\Proximity2Transit\Proximity2Transit.gdb\P_Proximity2050_NP"
Pprox_50NP_sdf = pd.DataFrame.spatial.from_featureclass(Pprox2050NP)
par_50=pd.read_csv(r"C:\Users\jhalpern\Desktop\C1-3\run98_parcel_data_2050.csv")


# In[11]:


#Add service level name
Pprox_50BP_sdf['Service_Level']=['No Fixed Route Transit','Bus 31+min','Bus 15-30min','Bus <15min','Major Transit Stop']
#Pprox_50NP_sdf['Service_Level']=['No Fixed Route Transit','Bus 31+min','Bus 15-30min','Bus <15min','Major Transit Stop']


# In[13]:


Pprox_50BP_sdf


# In[14]:


Pprox_50BP_sdf['MWTEMPN_share']=Pprox_50BP_sdf.SUM_FPSEMPN/par_50.FPSEMPN.sum()
Pprox_50BP_sdf['Scenario']='2050 Blueprint'


# In[15]:


Pprox_50BP_sdf


# In[44]:


#Calculate Shares
Pprox_50BP_sdf['tothh_share']=Pprox_50BP_sdf.SUM_tothh/par_50.tothh.sum()
Pprox_50BP_sdf['hhq1_share']=Pprox_50BP_sdf.SUM_hhq1/par_50.hhq1.sum()
Pprox_50BP_sdf['hhq2_share']=Pprox_50BP_sdf.SUM_hhq2/par_50.hhq2.sum()
Pprox_50BP_sdf['hhq3_share']=Pprox_50BP_sdf.SUM_hhq3/par_50.hhq3.sum()
Pprox_50BP_sdf['hhq4_share']=Pprox_50BP_sdf.SUM_hhq4/par_50.hhq4.sum()
Pprox_50BP_sdf['totemp_share']=Pprox_50BP_sdf.SUM_totemp/par_50.totemp.sum()
Pprox_50BP_sdf['RETEMPN_share']=Pprox_50BP_sdf.SUM_RETEMPN/par_50.RETEMPN.sum()
Pprox_50BP_sdf['MWTEMPN_share']=Pprox_50BP_sdf.SUM_MWTEMPN/par_50.MWTEMPN.sum()
Pprox_50BP_sdf['Scenario']='2050 Blueprint'


# In[45]:


Pprox_50NP_sdf['tothh_share']=Pprox_50NP_sdf.SUM_tothh/par_50.tothh.sum()
Pprox_50NP_sdf['hhq1_share']=Pprox_50NP_sdf.SUM_hhq1/par_50.hhq1.sum()
Pprox_50NP_sdf['hhq2_share']=Pprox_50NP_sdf.SUM_hhq2/par_50.hhq2.sum()
Pprox_50NP_sdf['hhq3_share']=Pprox_50NP_sdf.SUM_hhq3/par_50.hhq3.sum()
Pprox_50NP_sdf['hhq4_share']=Pprox_50NP_sdf.SUM_hhq4/par_50.hhq4.sum()
Pprox_50NP_sdf['totemp_share']=Pprox_50NP_sdf.SUM_totemp/par_50.totemp.sum()
Pprox_50NP_sdf['RETEMPN_share']=Pprox_50NP_sdf.SUM_RETEMPN/par_50.RETEMPN.sum()
Pprox_50NP_sdf['MWTEMPN_share']=Pprox_50NP_sdf.SUM_MWTEMPN/par_50.MWTEMPN.sum()
Pprox_50NP_sdf['Scenario']='2050 No Plan'


# In[21]:


#DONT ADD FOR LONG FORMAT
#Add suffix except Service Level
#Pprox_15_sdf = Pprox_15_sdf.add_suffix('_2015')
#prox_50NP_sdf = Pprox_50NP_sdf.add_suffix('_2050NP')
#Pprox_50BP_sdf = Pprox_50BP_sdf.add_suffix('_2050BP')
#Pprox_15_sdf.rename(columns={'Service_Level_2015':'Service_Level'},inplace=True)
#prox_50NP_sdf.rename(columns={'Service_Level_2050NP':'Service_Level'},inplace=True)
#Pprox_50BP_sdf.rename(columns={'Service_Level_2050BP':'Service_Level'},inplace=True)
#Set Service_Level as index for all outputs
#Pprox_15_sdf.set_index('Service_Level')
#prox_50NP_sdf.set_index('Service_Level')
#Pprox_50BP_sdf.set_index('Service_Level')


# In[16]:


#Combine
#for when 3 - df1.merge(df2,on='name').merge(df3,on='name')
proximity_parcel = pd.concat([Pprox_15_sdf,Pprox_50NP_sdf,Pprox_50BP_sdf], ignore_index=True,sort=False)


# In[17]:


proximity_parcel


# In[18]:


proximityexport=proximity_parcel[['Service_Level','Scenario','SUM_tothh','tothh_share','SUM_hhq1','hhq1_share',
                                  'SUM_totemp','totemp_share','SUM_RETEMPN','RETEMPN_share','SUM_MWTEMPN','MWTEMPN_share']]


# In[19]:


proximityexport


# In[20]:


proximityexport.to_csv(r'C:\Users\jhalpern\Desktop\C1-3\proximity2transit_parcelMWT.csv')

