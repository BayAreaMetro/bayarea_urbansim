
# Visualizer notes

This is published to Tableau Online: Modeling / Land Use Modeling / [BAUS_Visualizer](https://10ay.online.tableau.com/#/site/metropolitantransportationcommission/workbooks/1453751?:origin=card_share_link)

## Data Sources

### county_summary_growth
* Source:
  * `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\*_county_summary_growth.csv` inner joined (on *run_name* and *county*) with 
  * `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\*_county_dr_growth.csv` inner joined (on *run_name*) with 
  * `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\model_run_inventory.csv`
* Extract: `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\county_summary_growth.hyper`
* Used for:
  * Worksheet **County Output** which is part of [Dashboard **County Growth**](https://10ay.online.tableau.com/t/metropolitantransportationcommission/views/BAUS_Visualizer_17055142843640/CountyGrowth)

### taz_interim_output

* Source:  `M:\Data\Urban\BAUS\visualization_design\data\data_viz_ready\spatial\Travel Analysis Zones.shp` relate (`M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\*_interim_zone_output_allyears.csv` inner joined with `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\model_run_inventory.csv`)
* Extract: `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\taz_output.hyper`
* Used for:
  * Worksheet **TAZ Output Comparison Runs** which is part of [Dashboard **Taz Interim Output Comparison**](https://10ay.online.tableau.com/t/metropolitantransportationcommission/views/BAUS_Visualizer_17055142843640/TAZInterimComparison)

### taz_output
* Extract: `M:\urban_modeling\baus\PBA50Plus\BAUS_Visualizer_PBA50Plus_Files\PRODUCTION\taz_interim_output.hyper`
* Used for:
  * Worksheet **TAZ Output Comparison Runs** which is part of [Dashboard **Taz Output Comparison**](https://10ay.online.tableau.com/t/metropolitantransportationcommission/views/BAUS_Visualizer_17055142843640/TAZOutputComparison)