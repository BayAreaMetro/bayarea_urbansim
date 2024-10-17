# BAUS Metrics Scripts

These scripts generate plan performance metrics (currently, as of June 2024, for Plan Bay Area 2050+) based on BAUS model run outputs.

Earlier versions of the scripts came from Anup, @theocharides, and others. @lmz led the 2024 refactoring/refresh of the metrics scripts, with help from @akselx, @drewlevitt, @theocharides, and @nazanin87.

## Usage

```bash
python metrics_lu_standalone.py [rtp] [--no-interpolate] [--use_distinct_initial_year_data] [--test] [--only ...]
```

Command line arguments:
* `rtp`: "RTP2021" or "RTP2025". Controls which plan vintage (PBA50 / PBA50+) the script will process. Each plan vintage has specific [directories and control files](https://github.com/BayAreaMetro/bayarea_urbansim/blob/4a563674de85165f0116251f31e30e1b02703c6d/scripts/metrics/metrics_lu_standalone.py#L72) used to locate and identify model runs of interest. Most but not all metrics support RTP2021/PBA50 as well as RTP2025/PBA50+, which is useful to confirm that the revised metrics scripts produce outputs for PBA50 that are comparable to the published metrics from that plan.
* `--no-interpolate`: If `rtp` is "RTP2025", the script by default will [interpolate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/4a563674de85165f0116251f31e30e1b02703c6d/scripts/metrics/metrics_utils.py#L846) the base year to 2023. This is a simple linear interpolation of all numeric columns three-fifths of the way from their 2020 values to their 2025 values. As such, the "base year" data for RTP2025 is inherently a blend of observed and forecasted data. You can disable this behavior by passing `--no-interpolate`, which will make the script use 2020 data as the base year. This parameter has no effect if `rtp` is "RTP2021".
* `--use_distinct_initial_year_data`: Because the PBA50+ base year is by default an interpolation of 2020 and 2025 data, the "base year" data could vary from model run to model run, which is confusing. Therefore, by default, this script uses the base year data associated with the model run labeled as "No Project" in the PBA50+ [model run inventory](https://github.com/BayAreaMetro/bayarea_urbansim/blob/4a563674de85165f0116251f31e30e1b02703c6d/scripts/metrics/metrics_lu_standalone.py#L74) as the base year data for *all* PBA50+ model runs. You can disable this behavior by passing `--use_distinct_initial_year_data`, which will make the script use base year data specific to each model run. (This will require the script to read in 2020 and possibly also 2025 tables for all remaining model runs, which will make the script take a bit longer.) This parameter is ignored if `rtp` is "RTP2021".
* `--test`: Write output files to the local directory, instead of [`OUTPUT_PATH`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/4a563674de85165f0116251f31e30e1b02703c6d/scripts/metrics/metrics_lu_standalone.py#L75). Useful primarily for testing.
* `--only ...`: Passing one of ['affordable', 'connected', 'diverse', 'growth', 'healthy', 'vibrant'] will cause the script to compute only the specified types of metrics. Useful primarily for testing.

The idea is that most arguments will not be needed for production use, and `--no-interpolate` and `--use_distinct_initial_year_data` will generally not be needed at all. (`--no_interpolate` can be helpful if you are calculating metrics for an older PBA50+ model run that didn't write out 2025 tables.) But most of the time you will just be able to run commands like
```bash
python metrics_lu_standalone.py RTP2025 --test --only diverse
```
or even simply
```bash
python metrics_lu_standalone.py RTP2025
```

## Linear interpolation to 2023 base year

By default, when calculating metrics for PBA50+ model runs, this script [linearly interpolates](https://github.com/BayAreaMetro/bayarea_urbansim/blob/4a563674de85165f0116251f31e30e1b02703c6d/scripts/metrics/metrics_utils.py#L846) between 2020 and 2025 values to calculate 2023 values, which are then used as the "base year" data. 

This is a simple process in which all numeric columns in the parcel-level, TAZ-level, and county-level tables are interpolated three-fifths of the way from their 2020 values to their 2025 values. This process works because (as of June 2024) all numeric columns represent simple headcounts rather than percentages or other non-headcount number types that would require weighted averaging or other more complex operations to correctly interpolate. In general, the strategy of retaining simple headcounts as late into the workflow as possible, then deriving percentages etc immediately before reporting final metrics, works well for situations like this one.
