### Overview

Bay Area UrbanSim documentation is now being created via [mkdocs](https://www.mkdocs.org), with multiple versions deployed (by branch) via [mike](https://github.com/jimporter/mike).

Different versions of the documentation are stored in the various folders in this repo, e.g. "main"; folder names are the same as the branch names. The Github documentation page [http://bayareametro.github.io/bayarea_urbansim/](http://bayareametro.github.io/bayarea_urbansim/) has a drop-down menu of versions.

In the current config, the Github documentation page defaults to the most recently updated version. This is achieved by automatically syncing the latest updates into the "latest" folder.

A legacy version of the documentation is saved in the [`legacy_gh_pages`](https://github.com/BayAreaMetro/bayarea_urbansim/tree/legacy_gh_pages) branch for reference.

### Setup and update branch-versioned documentation

#### Install packages

* mkdocs: `pip install mkdocs`
* mike: `pip install mike`, `pip install mkdocs-autorefs`
* mkdocs "material" template: `pip install mkdocs-material`
* docstring support: `pip install mkdocstrings[python]`

#### Create and update documentation for a non-documentation branch

Below uses a hypothetical branch named "baus_v2" as an example:
* while in branch "baus_v2", edit the `mkdocs.yml` file located in the repo's root dir, and the markdown files located in the "docs" folder
* in Anaconda Prompt window, cd to the root dir ("bayarea_urbansim" dir of the said branch), call `mike deploy [branch_name] [alias=latest]` or `mike deploy [branch_name]` (if another branch has the alias "latest") , which will create a Github commit in branch "gh-pages". Two situations:
	* if this is the initial documentation publication of branch "baus_v2", it will create a new folder called "baus_v2" in the root dir of the "gh-pages" branch to host documentation contents, and also update the "latest" folder.  
	* if this is updating the already published documentation of branch "baus_v2", it will push the updates to both folder "baus_v2" and folder "latest" in branch "gh-pages".
* switch to branch "gh-pages", and push the commit to origin.

Note that when mkdocs or mike deploys a documentation creation or update, it will create or update a folder called "site" to store documentation builds in the non-documentation branch. No need to check the documentation builds into the repository, therefore, may consider adding `site/` to `.gitignore`.

To then make a branch's documentation the "main" branch's documentation:
* merge the branch into main (the edit will go into main as well)
* deploy the `main` branch

Other userful commands:
* `mike list`: listing all currently-deployed doc versions including their alias
* `mike set-default [version-or-alias]`: set one of the deployed doc version as the default
when launching the documentation site
* `mike delete [version-or-alias]`: after merging a branch (e.g. "baus_v2") into the main branch and deleting that branch, recommand also deleting the associated doc, e.g. `mike delete baus_v2`
