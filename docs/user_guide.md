# User Guide

This User Guide applies to UrbanSim implementation for the Bay Area.

## Installation

Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended.
1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository
3. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
4. Activate the environment: `conda activate baus-env-2020`

## Model Run

1. Download the model inputs folder and run_setup.yaml file (ask an MTC contact for access) and create an outputs directory
3. Specify I/O folder locations, model features to enable, and policy configurations in `run_setup.yaml`
4. Run `python baus.py` from the main model directory (more info about the command line arguments: `python baus.py --help`)

## Optional visualization tool and Slack messenger

* Configure Amazon Web Services (AWS) to get s3 permission (you will need an appropriately configured AWS credentials file from your MTC contact)
* Install AWS SDK for Python -- boto3 using `pip install boto3`
* Install Slacker to use Slack API using `pip install slacker` (you will need an appropriate slack token to access the slack bot from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`

## Adding documentation to gh-pages

1. Install the required docs packages with pip: `mkdocs`, `mike`, `mkdocs-autorefs`, `mkdocs-material`, `mkdocstrings[python]*`
2. While in your development branch, edit the mkdocs.yml file located in the repo's root dir and the markdown files located in the `docs` folder
3. From the mode's root dir call `mike deploy [branch_name] [alias=latest] or mike deploy [branch_name]` (if another branch has the alias "latest"), which will create a Github commit in branch "gh-pages". If this is the initial documentation publication of the branch it will create a new folder in the root dir of the "gh-pages" branch using the branch name and also update the "latest" folder otherwise, it will push the updates to both folder "baus_v2" and folder "latest" in branch "gh-pages".
4. Switch to branch "gh-pages", and push the commit to origin.
5. To make a branch's documentation the `main` branch's documentation, merge the branch into main and deploy the main branch.
6. After merging a branch into the main branch and deleting that branch, delete the associated doc using `mike delete [branch_name]`