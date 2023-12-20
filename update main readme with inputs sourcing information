Bay Area UrbanSim (BAUS) Implementation

This is the UrbanSim implementation for the Bay Area. Documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/). All documentation for Bay Area Urbansim is at: http://bayareametro.github.io/bayarea_urbansim/main/

## Installation
Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended. 

1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository 
3. Create a Python environment with the current dependencies: `conda env create -f baus-env-2023.yml`
4. Activate the environment: `conda activate baus-env-2023`
5. Store a `run_setup.yaml` file in the repository's main directory that contains the model run configuration
6. Use `run_setup.yaml` to specify a `run_name` for your model run
7. Use `run_setup.yaml` to set a path to source model inputs from (stored on MTC's servers for internal use)
8. Use `run_setup.yaml` to set a path for model outputs to write to (it's helpful if the outputs folder match the model run name)
9. Run `python baus.py` from the main model directory (more info about the command line arguments: `python baus.py --help`)


## Optional Slack Messenger 
* Install the Slack SDK using `pip install slack_sdk`
* Set environment variable `SLACK_TOKEN = token` (you will need an appropriate slack token from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`


## Optional Model Run Visualizer
* Configure the location that BAUS will write the visualizer files to in `run_setup.yaml` (stored on MTC's servers for internal visualization)
* Open the visualizer from the BAUS repository to explore the model run, and/or
* Open the visualizer from the BAUS repository and publish it to the web (hosted on MTC's Tableau account). At this time runs can be removed from `model_run_inventory.csv` to select the runs to be shown on the web tool


## Documentation
* See the repository's `gh-pages` branch for instructions on installing the BAUS documentation packages and submitting documentation
