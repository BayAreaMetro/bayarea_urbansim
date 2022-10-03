Bay Area UrbanSim (BAUS) Implementation
=======

[![Build Status](https://travis-ci.org/UDST/bayarea_urbansim.svg?branch=master)](https://travis-ci.org/UDST/bayarea_urbansim)

Policy documentation for the Bay Area model is available [here](http://data.mtc.ca.gov/bayarea_urbansim/) and documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/).

### Branches

* `main` contains the most recent release
* `develop` is a staging branch used in high production phases (generally Plan applicaton)
* feature branches contain descriptive names
* application branches contain descriptive names

### Installation

Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended.

1. Install the [Anaconda Python](https://www.anaconda.com/products/individual#Downloads) distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository
3. Download base data from this [Box folder](https://mtcdrive.app.box.com/folder/103451630229?s=di77ya24xmmsg3rkrotaewegzsd61hu6) and move the files to `bayarea_urbansim/data/` (ask an MTC contact for access)
4. Clone the MTC [urban_data_internal repository](https://github.com/BayAreaMetro/urban_data_internal) to the same location as this repository (ask an MTC contact for access)
5. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
6. Activate the environment: `conda activate baus-env-2020`
7. Pre-process the base data: `python baus.py --mode preprocessing` (only needed once)
8. Run the model: `python baus.py` (typical AWS linux run uses `nohup python baus.py -s 25 --disable-slack --random-seed &` which add no hanging up / specifies scenario 25 / disables slack output / turns OFF random seed / puts in background)

More info about the command line arguments: `python baus.py --help`

Optional visualization tool and Slack messenger: 
* Configure Amazon Web Services (AWS) to get s3 permission (you will need an appropriately configured AWS credentials file from your MTC contact) 
* Install AWS SDK for Python -- boto3 using `pip install boto3`
* Install Slacker to use Slack API using `pip install slacker` (you will need an appropriate slack token to access the slack bot from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`

### An overview of baus.py
 
baus.py is a command line interface (cli) used to run Bay Area UrbanSim in various modes.  These modes currently include:

* estimation, which runs a series of models to save parameter estimates for all statistical models
* simulation, which runs all models to create a simulated regional growth forecast
* fetch_data, which downloads large data files from Amazon S3 as inputs for BAUS
* preprocessing, which performas long-running data cleaning steps and writes newly cleaned data back to the binary h5 file for use in the other steps
* baseyearsim which runs a "base year simulation" which summarizes the data before the simulation runs (during simulation, summaries are written after each year, so the first year's summaries are *after* the base year is finished - a base year simulation writes the summaries before any models have run)

### Urban Analytics Lab (UAL) Improvements

#### Data schemas

* Builds out the representation of individual housing units to include a semi-persistent tenure status, which is assigned based on characteristics of initial unit occupants
* Joins additional race/ethnicity PUMS variables to synthetic households [NB: currently missing from the reconciled model, but will be re-added]
* Adds a representation of market rents alongside market sale prices

#### Model steps

* Residential hedonics predict market rents and sale prices separately, with rents estimated from Craigslist listings
* Household move-out choice is conditional on tenure status
* Household location choice is modeled separately for renters and owners, and includes race/ethnicity measures as explanatory variables
* Developer models are updated to produce both rental and ownership housing stock

Notebooks, work history, code samples, etc are kept in a separate [bayarea_urbansim_work](https://github.com/ual/bayarea_urbansim_work) repository. 

#### Current status (August 2016)

* All of the UAL alterations have been refactored as modular orca steps
* This code is contained in `baus/ual.py`, `configs/ual_settings.yaml` and individual `yaml` files as needed for regression models that have been re-estimated
* There are *no* changes to `urbansim`, `urbansim_defaults`, or MTC's orca initialization and model steps
* MTC and UAL model steps can be mixed and matched by passing different lists to orca; see `run.py` for examples
* The UAL model steps document and test for required data characteristics, using the [orca_test](https://github.com/udst/orca_test) library

### Outputs from Simulation (written to the runs directory)

`[num]` = a positive integer used to identify each successive run.  This number usually starts at 1 and increments each time baus.py is called.
Many files are output to the `runs/` directory. They are described below.

filename |description
----------------------------|-----------
run[num]_parcel_output.csv 		| A csv of all new built space in the region, not including ADUs added to existing buildings.  This has a few thousand rows and dozens of columns which contain various inputs and outputs, as well as debugging information which helps explain why each development was picked by UrbanSim.
run[num]\_parcel_data\_[year].csv 			|A CSV with parcel level output for *all* parcels with lat, lng and includes change in total_residential_units and change in total_job_spaces, as well as zoned capacity measures.
run[num]\_building_data\_[year].csv 			|The same as above but for buildings.
run[num]\_taz\_summaries\_[year].csv 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel)
run[num]\_pda_summaries\_[year].csv, run[num]\_juris_summaries\_[year].csv, run[num]\_superdistrict_summaries\_[year].csv | Similar outputs to the taz summaries but for each of these geographies.  Used for understanding the UrbanSim forecast at an aggregate level.
run[num]\_acctlog_[account name]\_[year].csv    |A series of CSVs of each account's funding amount and buildings developed under this acount (if the funding is used to subsidize development) in each iteration.
run[runnum]_dropped_buildings.csv     | A summary of buildings which were redeveloped during the simulated forecast.
run[runnum]_simulation_output.json | Used by the web output viewer.

### Directory structure

* baus/ contains all the Python code which runs the BAUS model.
* data/ contains BAUS inputs which are small enough to store and render in GitHub (large files are stored on Amazon S3) - this also contains lots of scenario inputs in the form of csv files.  See the readme in the data directory for detailed docs on each file.
* configs/ contains the model configuration files used by UrbanSim.  This also contains settings.yaml which provides simulation inputs and settings in a non-tabular form. 
* scripts/ these are one-off scripts which are used to perform various input munging and output analysis tasks.  See the docs in that directory for more information.

## Data management

* Add manual_edits.csv to edit individual attributes for each building/parcel in the building and parcel tables
* Do job assignment by starting with baseyear controls and assigning on the fly (should do the same for households)

## Run management

* Ability to run different model sets, output to Slack and web maps (in baus.py)

## Standard (or extensions to) UrbanSim features

* Houshold and job control totals by year
* Standard UrbanSim models - hedonic, location choice, transition, relocation models
* Basic supply and demand interactions to raise prices where demand exceeds supply
* Separate low and high income hlcms for the deed restricted units
* 1300 lines of computed columns to support the functionality described above

### Accessibility variables

* Accessibility variables including special "distance to location" variables
* Both local (local street network from OSM) and regional (travel model network) networks
* Network aggregations for prices as well

## Human input and overrides for specific models

* Do parcel-by-parcel rejections based on human knowledge to override the model
* Quite a bit of support by year and by scenario for custom development projects, as well as add vs demolish settings
* Proportional job model on "static parcels".  Static parcels are parcels whose jobs do not enter the relocation model and are auto-marked nodev.  These are e.g. city hall and Berkeley which would never get newly created by the model
* Relocation rates by taz

## Developer model

* Provide baseline zoning using parcel to zoning lookups and then attributes for all the zones in the region (num zones << num parcels)
* Do conditional upzoning and downzoning, add and remove building types, all by policy zone
* Limits - assign all parcels to a "bucket" - in this case to a city, but it could be a pda-city intersection.  Limits the amount of res and non-res development that comes from each bucket.  Make sure to do so for all non-res types.
* A special retail model which takes into account where demand for retail is high (income x households) but supply is low
* We still need a better way to determine the uses that get built between office / retail / industrial / whatever else
* A reprocessing of the developer results which adds job spaces to residential buildings at an appropriate rate, a second part of this adds ground floor retail by policy for tall buildings in order to create retail where people live

## Tweaks to get more reasonable results

* A setting which allows a user to disable building a more dense buiding that currently exists in a taz (used when we don't trust zoning much) - we minimize the max_dua or far on each parcel with the max built_dua or far near to that parcel
* For lack of better land price data, we use land prices proportional to the prevailing price per sqft
* Add (large) factors to land which is industrial to account for expensive land preparation
* Price shifters and cost shifters
* Rules to not consider certain parcels for development, like parcels which contain historical buildings, or have single family homes on small lots


## Output summaries, analysis and visualization

* Geographic summaries of households, jobs, and buildings for each 5 year increment, for superdistrict, pda, and juris geographies
* Write out disaggregate parcel and building data for visualization with ESRI
* A utility to compare output summary tables and put them in separate tabs of a workbook and color-code large differences
* Back into some demographic variables needed by the travel model that aren't produced by our UrbanSim model, including age categories, employed residents (by household location), population (we do households) some legacy land use variables from the previous model
* The travel model output which summarized some basic variables by zone
* Write out account information (subsididies and fees)
* Do urbanfootprint accounting - whether development is inside or outside the "UGB"
* Some extra diagnostic accounting including simple capacity calculations, simulated vacancy rates (for debugging, not as a sim output), sqft by building type
* Compare household and job counts to abag targets at the pda and juris levels

