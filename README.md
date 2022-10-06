Bay Area UrbanSim documentation is now being created via [mkdocs](https://www.mkdocs.org), with multiple
versions deployed (by branch) via [mike](https://github.com/jimporter/mike).

A legacy version of the documentation is saved in the [`legacy_gh_pages`](https://github.com/BayAreaMetro/bayarea_urbansim/tree/legacy_gh_pages) branch for reference.

To update documentation for a non-documentation branch, e.g. "main", "develop":
* install the mkdocs and mike packages: `pip install mkdocs`, `pip install mike`, `pip install mkdocs-autorefs`
* edit `mkdocs.yml` located in the repo's root dir, and the markdown files located in the "docs" folder
* in cmd window, cd to the root dir, call `mike deploy [branch_name]`