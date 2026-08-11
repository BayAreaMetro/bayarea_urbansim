"""Train and validate the block residential developer location-choice model.

Fits the block residential allocation LCM: a binomial GLM whose chooser
is a new increment of residential development and whose alternatives are census
blocks, estimated on the 2010->2020 block housing-unit-change dataset assembled
by the sibling `block_developer_data_assembly` module.

Mirrors the k-fold cross-validation design used for the affordable-housing
developer LCM (see `fms-notebook-hub/folumpp/08_ja_affordable_housing_dev.qmd`),
adapted for a non-rare outcome: the block dependent variable (`developed`,
whether a block's 2010->2020 net housing-unit change is positive) occurs in
roughly 30% of blocks, so folds are stratified on county x developed jointly
rather than splitting only the positive class across a shared background
choice set.

The model is **estimated at block grain but validated at tract
grain** -- transferring tract-scale coefficients to blocks would be a
change-of-support / MAUP liability, so a block-grain fit is paired with
tract-level aggregate diagnostics (slope, intercept, R-squared, RMSE) analogous
to the affordable model's superdistrict validation. Validation covers **location
only** -- whether the model generally chooses the right blocks to develop --
not unit counts: unit counts are
decided by the feasibility-derived deliverable-capacity roll-up, not by this
model's predicted probabilities.

Unlike the affordable-housing developer notebook (which standardizes features
with `StandardScaler` before fitting), this trainer fits directly on the raw
covariate values. No BAUS submodel persists a scaler at simulation time --
HLCM/ELCM/hedonic yamls store a patsy `model_expression` (transforms embedded,
e.g. `np.log1p(unit_residential_price)`) plus raw-scale `fit_parameters`,
and urbansim builds the design matrix from live raw columns each simulated year
(see `configs/location_choice/hlcm_owner.yaml`). Fitting unstandardized here
keeps the exported coefficients in that same raw space, so the eventual
simulation step can score live blocks with a plain `model_expression` eval --
no scaler to carry into `baus/block_developer.py`.

Alternative specifications are compared by adding an entry to
`BLOCK_DEVELOPER_SPECS` and calling `compare_specs`; the fitted coefficients
for a chosen spec are written to a spec yaml in the shape of
`configs/location_choice/hlcm_owner.yaml`.

Usage::

    # From the repo root (registers the `scripts` package for the sibling import):
    python -m scripts.block_port.block_developer_train

    # Or, in memory:
    from scripts.block_port.block_developer_train import train_block_developer
    result = train_block_developer(spec_name="baseline", census_api_key="YOUR_KEY")
"""

import pathlib
import os

import numpy as np
import pandas as pd
import statsmodels.api as sm
import yaml
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold
from sklearn.utils.class_weight import compute_sample_weight

from scripts.block_port.block_developer_data_assembly import build_block_developer_dataset

__all__ = [
    # Building blocks -- prep the estimation frame and CV folds
    'prepare_block_estimation_frame',
    'build_fold_indices',
    # Core fit + cross-validation
    'fit_and_evaluate_block',
    'run_kfold_cv',
    # Validation -- aggregate block predictions to tract for MAUP-safe diagnostics
    'compute_tract_validation',
    # Spec comparison -- add a BLOCK_DEVELOPER_SPECS entry to compare an alternative
    'compare_specs',
    # Export -- write fitted coefficients to the spec yaml
    'export_spec_yaml',
    # Primary entry point -- assemble, fit, validate, and export a single spec
    'train_block_developer',
]

# ---------------------------------------------------------------------------- #
#                                  Constants                                    #
# ---------------------------------------------------------------------------- #

# A block is "developed" if its net 2010->2020 housing-unit change is positive.
_DEVELOPED_THRESHOLD = 0

# ── K-fold CV configuration ──────────────────────────────────────────────────
N_FOLDS = 5
KFOLD_RANDOM_STATE = 42

# Fitted spec yaml destination, in the shape of configs/location_choice/*.yaml.
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SPEC_YAML_PATH = _REPO_ROOT / 'configs' / 'developer' / 'block_residential_developer.yaml'

# Candidate model_expression variants. Add a key here to define an alternative
# spec; compare_specs() and train_block_developer(spec_name=...) both read from
# this registry.
BLOCK_DEVELOPER_SPECS = {
    'baseline': [
        'log_land_value', 'log_parcel_acres',  # Site characteristics
        'jobs_15',  # Local job accessibility
        'ave_income_500', 'residential_units_1500',  # Neighborhood context
        'log_retail_sqft_3000',  # Retail access
        'zoned_du_build_ratio',  # Block deliverable-capacity signal
    ],
}


# ---------------------------------------------------------------------------- #
#                     Building blocks: frame prep + CV folds                   #
# ---------------------------------------------------------------------------- #

def prepare_block_estimation_frame(estimation_frame):
    """Prepares the block estimation frame for location-choice model fitting.

    Adds the binarized dependent variable (`developed`, whether a block's net
    2010->2020 housing-unit change is positive) and the 11-char tract GEOID
    (the first 11 characters of the 15-char block GEOID) used for tract-level
    validation.

    Args:
        estimation_frame: The block-indexed frame returned by
            `build_block_developer_dataset`, with a `hu_change` column and a
            `block_geoid` index.

    Returns:
        A copy of `estimation_frame` with two added columns: `developed`
        (int, 0/1) and `tract_geoid` (str).

    Example:
        >>> frame = prepare_block_estimation_frame(estimation_frame)
        >>> frame['developed'].isin([0, 1]).all()
        True

    See Also:
        build_fold_indices: consumes the `developed` and county columns this
            adds to build stratified CV folds.
    """
    frame = estimation_frame.copy()
    frame['developed'] = (frame['hu_change'] > _DEVELOPED_THRESHOLD).astype(int)
    frame['tract_geoid'] = frame.index.str[:11]
    return frame


def build_fold_indices(frame, n_folds=N_FOLDS, random_state=KFOLD_RANDOM_STATE):
    """Builds stratified k-fold cross-validation splits over blocks.

    Stratifies jointly on county and the binarized `developed` outcome, so
    each fold preserves both the regional county mix and the ~30% block
    development rate. This differs from the affordable-housing developer
    model's fold design, which splits only the rare positive class across a
    shared background choice set -- unnecessary here since `developed` is not
    a rare outcome.

    Args:
        frame: The prepared block estimation frame from
            `prepare_block_estimation_frame`, with `county` and
            `developed` columns.
        n_folds: Number of cross-validation folds.
        random_state: Random seed for fold assignment reproducibility.

    Returns:
        A list of `(train_idx, test_idx)` tuples of positional (`iloc`)
        indices into `frame`, as returned by `StratifiedKFold.split`.

    Example:
        >>> fold_indices = build_fold_indices(frame)
        >>> len(fold_indices)
        5

    See Also:
        run_kfold_cv: consumes these fold indices to fit and evaluate a spec.
    """
    stratify_labels = frame['county'].astype(str) + '_' + frame['developed'].astype(str)
    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=random_state)
    return list(skf.split(frame, stratify_labels))


# ---------------------------------------------------------------------------- #
#                       Core fit + cross-validation                            #
# ---------------------------------------------------------------------------- #

def _add_significance_stars(p_value):
    """Maps a p-value to its conventional significance-star annotation.

    Args:
        p_value: A coefficient's two-sided p-value.

    Returns:
        `'***'` for p < 0.001, `'**'` for p < 0.01, `'*'` for p < 0.05,
        else `''`.
    """
    if p_value < 0.001:
        return '***'
    elif p_value < 0.01:
        return '**'
    elif p_value < 0.05:
        return '*'
    return ''


def fit_and_evaluate_block(feature_vars, train_set, test_set, name):
    """Fits a GLM Binomial block location-choice model and evaluates it out-of-sample.

    Fits a balanced-weighted GLM Binomial via `statsmodels` directly on the raw
    (unstandardized) covariate values and scores AUC on both the training and
    test sets. Unlike the affordable-housing developer notebook this mirrors, no
    `StandardScaler` is applied: BAUS's own HLCM/ELCM/hedonic yamls store
    raw-scale `fit_parameters` with no persisted scaler, so fitting
    unstandardized here keeps the exported coefficients directly usable by a
    simulation-time `model_expression` eval over live block covariates.

    Args:
        feature_vars: Variable names as they appear in `train_set`/`test_set`.
        train_set: Training-fold blocks.
        test_set: Test-fold blocks.
        name: Model identifier (e.g. `"baseline fold 1"`); appended to the
            feature list to form the printed label.

    Returns:
        A dict with keys: `label`, `glm_result`, `model_features`,
        `train_auc`, `test_auc`, `probs` (test-set predicted
        probabilities, aligned to `test_set` row order), `summary_df`
        (coefficient table, in raw covariate units).

    Example:
        >>> result = fit_and_evaluate_block(
        ...     BLOCK_DEVELOPER_SPECS['baseline'], train_set, test_set, 'baseline fold 1')
        >>> 0 <= result['test_auc'] <= 1
        True

    See Also:
        run_kfold_cv: calls this once per fold plus once on the full dataset.
    """
    label = f"{name}: {', '.join(feature_vars)}"

    X_train = train_set[feature_vars].fillna(0)
    y_train = train_set['developed']

    X_train_with_const = sm.add_constant(X_train)

    sample_weights = compute_sample_weight('balanced', y_train)

    glm_result = sm.GLM(
        y_train,
        X_train_with_const,
        family=sm.families.Binomial(),
        var_weights=sample_weights,
    ).fit(disp=0)

    y_train_pred = glm_result.predict(X_train_with_const)
    train_auc = roc_auc_score(y_train, y_train_pred)

    X_test = test_set[feature_vars].fillna(0)
    y_test = test_set['developed']
    y_test_pred = glm_result.predict(sm.add_constant(X_test, has_constant='add'))
    test_auc = roc_auc_score(y_test, y_test_pred)

    probs = np.asarray(y_test_pred)

    summary_df = pd.DataFrame({
        'Variable': ['Intercept'] + feature_vars,
        'Coefficient': glm_result.params.values,
        'Std Error': glm_result.bse.values,
        'z-value': glm_result.tvalues.values,
        'P>|z|': glm_result.pvalues.values,
    })
    summary_df['Sig'] = summary_df['P>|z|'].apply(_add_significance_stars)

    print(f"\n{'=' * 80}\n{label}\n{'=' * 80}")
    pd.options.display.float_format = '{:.4f}'.format
    print(summary_df[['Variable', 'Coefficient', 'Std Error', 'z-value', 'P>|z|', 'Sig']]
          .to_string(index=False))
    print(f"Train AUC: {train_auc:.4f}  |  Test AUC: {test_auc:.4f}  ({test_auc - train_auc:+.4f})")
    print(f"Log-Likelihood: {glm_result.llf:.2f}  |  AIC: {glm_result.aic:.2f}  |  "
          f"BIC: {glm_result.bic_llf:.2f}")

    return dict(
        label=label,
        glm_result=glm_result,
        model_features=feature_vars,
        train_auc=train_auc,
        test_auc=test_auc,
        probs=probs,
        summary_df=summary_df,
    )


def run_kfold_cv(feature_vars, name, frame, fold_indices):
    """Runs k-fold cross-validation for a block location-choice spec.

    For each fold, trains on the fold's training blocks and evaluates on its
    test blocks, assembling out-of-fold (OOF) predicted probabilities across all
    folds (every block appears in exactly one test fold, unlike the affordable
    model's rare-event design). A final model is also fit on the full dataset
    for export.

    Args:
        feature_vars: RHS variable names as they appear in `frame`.
        name: Model identifier string (e.g. `"baseline"`).
        frame: The prepared block estimation frame.
        fold_indices: List of `(train_idx, test_idx)` tuples into `frame`,
            as returned by `build_fold_indices`.

    Returns:
        A dict with keys:
            - `cv_results`: list of per-fold dicts from `fit_and_evaluate_block`.
            - `mean_test_auc` / `std_test_auc`: float, across folds.
            - `oof_probs`: Series of OOF predicted probabilities indexed by
              `block_geoid`.
            - `final_model`: dict from `fit_and_evaluate_block` fit on the
              full dataset.

    Example:
        >>> cv_baseline = run_kfold_cv(
        ...     BLOCK_DEVELOPER_SPECS['baseline'], 'baseline', frame, fold_indices)
        >>> print(f"{cv_baseline['mean_test_auc']:.3f} +/- {cv_baseline['std_test_auc']:.3f}")

    See Also:
        compute_tract_validation: consumes `oof_probs` for tract-level checks.
        compare_specs: calls this once per spec in `BLOCK_DEVELOPER_SPECS`.
    """
    cv_results = []
    oof_prob_parts = []

    for fold, (train_idx, test_idx) in enumerate(fold_indices):
        train_set = frame.iloc[train_idx]
        test_set = frame.iloc[test_idx]

        result = fit_and_evaluate_block(feature_vars, train_set, test_set, f'{name} fold {fold + 1}')
        cv_results.append(result)

        oof_prob_parts.append(pd.Series(result['probs'], index=test_set.index, name='oof_prob'))

    test_aucs = [r['test_auc'] for r in cv_results]
    mean_test_auc = float(np.mean(test_aucs))
    std_test_auc = float(np.std(test_aucs))
    oof_probs = pd.concat(oof_prob_parts).reindex(frame.index)

    final_model = fit_and_evaluate_block(feature_vars, frame, frame, f'{name} (full)')

    print(f"\n{'=' * 60}\n{name} — {len(fold_indices)}-Fold CV Results (stratified by county x developed)\n{'=' * 60}")
    for fold, r in enumerate(cv_results):
        print(f"  Fold {fold + 1}: Train AUC {r['train_auc']:.4f}  |  Test AUC {r['test_auc']:.4f}")
    print(f"  Mean Test AUC: {mean_test_auc:.4f} +/- {std_test_auc:.4f}")

    return dict(
        cv_results=cv_results,
        mean_test_auc=mean_test_auc,
        std_test_auc=std_test_auc,
        oof_probs=oof_probs,
        final_model=final_model,
    )


# ---------------------------------------------------------------------------- #
#                        Tract-level aggregate validation                      #
# ---------------------------------------------------------------------------- #

def _compute_fit_stats(observed, predicted):
    """Computes slope, intercept, R-squared, and RMSE for an observed-vs-predicted fit.

    Args:
        observed: Array-like of observed values.
        predicted: Array-like of predicted values, aligned to `observed`.

    Returns:
        A dict with keys `slope`, `intercept`, `r2`, `rmse`, `n`.
    """
    slope, intercept = np.polyfit(observed, predicted, 1)
    y_hat = slope * observed + intercept
    r2 = np.corrcoef(observed, predicted)[0, 1] ** 2
    rmse = np.sqrt(np.mean((predicted - y_hat) ** 2))
    return dict(slope=float(slope), intercept=float(intercept), r2=float(r2),
                rmse=float(rmse), n=len(observed))


def compute_tract_validation(frame, oof_probs):
    """Validates block-grain predictions by aggregating them to census tracts.

    The block LCM is estimated at block grain but validated at
    tract grain: transferring tract-scale coefficients to blocks would be a
    change-of-support / MAUP liability, so tract-level aggregate checks manage
    block-level noise instead. This validates location only -- **propensity**:
    observed count of developed blocks vs. the OOF predicted probabilities
    summed per tract (expected developed-block count), scored with slope,
    intercept, R-squared, and RMSE, mirroring the affordable-housing developer
    model's superdistrict validation.

    Unit counts are intentionally out of scope here: "how many units per block"
    is decided by the
    feasibility-derived deliverable-capacity roll-up, not by this location-choice
    model's predicted probabilities -- so there is no probability-weighted unit
    allocation to validate at this stage.

    Args:
        frame: The prepared block estimation frame, with a `developed` column
            and a `tract_geoid` column.
        oof_probs: OOF predicted probabilities indexed by `block_geoid`, as
            returned by `run_kfold_cv`.

    Returns:
        A dict with keys `tract_agg` (the tract-level aggregate DataFrame) and
        `propensity_stats` (a dict from `_compute_fit_stats`).

    Example:
        >>> tract_validation = compute_tract_validation(frame, cv_result['oof_probs'])
        >>> tract_validation['propensity_stats']['r2'] > 0
        True

    See Also:
        run_kfold_cv: produces the `oof_probs` this consumes.
    """
    validation_frame = pd.DataFrame({
        'tract_geoid': frame['tract_geoid'],
        'observed_developed': frame['developed'],
        'predicted_developed': oof_probs,
    })
    tract_agg = validation_frame.groupby('tract_geoid')[
        ['observed_developed', 'predicted_developed']
    ].sum()

    propensity_stats = _compute_fit_stats(
        tract_agg['observed_developed'].values, tract_agg['predicted_developed'].values)

    return dict(tract_agg=tract_agg, propensity_stats=propensity_stats)


# ---------------------------------------------------------------------------- #
#                              Spec comparison                                 #
# ---------------------------------------------------------------------------- #

def compare_specs(frame, fold_indices, specs=None):
    """Fits, validates, and tabulates every spec in a spec registry.

    Runs `run_kfold_cv` and `compute_tract_validation` for each entry in
    `specs` and assembles a tidy comparison table. To compare an alternative
    specification, add an entry to `BLOCK_DEVELOPER_SPECS` (or pass a custom
    `specs` dict) rather than modifying this function.

    Args:
        frame: The prepared block estimation frame.
        fold_indices: List of `(train_idx, test_idx)` tuples from
            `build_fold_indices`.
        specs: Mapping of spec name to feature-variable list. Defaults to
            `BLOCK_DEVELOPER_SPECS`.

    Returns:
        A dict with keys `cv_results` (mapping of spec name to a dict of the
        `run_kfold_cv` result plus `tract_validation`) and
        `comparison_table` (a DataFrame indexed by spec name with
        `mean_test_auc`, `std_test_auc`, and `tract_propensity_r2` columns).

    Example:
        >>> comparison = compare_specs(frame, fold_indices,
        ...     specs={'baseline': BLOCK_DEVELOPER_SPECS['baseline'], 'alt': alt_vars})
        >>> comparison['comparison_table'].loc['baseline', 'mean_test_auc'] > 0.5
        True

    See Also:
        train_block_developer: fits, validates, and exports a single chosen spec.
    """
    specs = specs if specs is not None else BLOCK_DEVELOPER_SPECS
    cv_results = {}
    comparison_rows = []

    for spec_name, feature_vars in specs.items():
        cv_result = run_kfold_cv(feature_vars, spec_name, frame, fold_indices)
        tract_validation = compute_tract_validation(frame, cv_result['oof_probs'])
        cv_results[spec_name] = dict(cv_result, tract_validation=tract_validation)
        comparison_rows.append(dict(
            spec=spec_name,
            mean_test_auc=cv_result['mean_test_auc'],
            std_test_auc=cv_result['std_test_auc'],
            tract_propensity_r2=tract_validation['propensity_stats']['r2'],
        ))

    comparison_table = pd.DataFrame(comparison_rows).set_index('spec')
    print(f"\n{'=' * 60}\nSpec comparison\n{'=' * 60}")
    print(comparison_table.to_string(float_format='{:.4f}'.format))

    return dict(cv_results=cv_results, comparison_table=comparison_table)


# ---------------------------------------------------------------------------- #
#                                   Export                                     #
# ---------------------------------------------------------------------------- #

def export_spec_yaml(final_model, spec_name, tract_validation=None, path=_SPEC_YAML_PATH):
    """Writes a fitted block-developer spec to a location-choice-style yaml.

    Follows the `model_expression` + `fit_parameters` shape used by
    `configs/location_choice/hlcm_owner.yaml`, with `fit_parameters` holding
    `Coefficient`, `Std. Error`, and `T-Score` keyed by variable name
    (including `Intercept`). Coefficients are in raw covariate space (no
    scaler was applied during fitting -- see `fit_and_evaluate_block`), so a
    simulation-time step can score live blocks with a plain `model_expression`
    eval, matching how BAUS's own HLCM/ELCM/hedonic yamls are consumed. Unlike
    the HLCM/ELCM yamls, this is a bespoke GLM Binomial fit rather than an
    `urbansim_defaults` MNL, so it is not yet consumed by `utils.lcm_estimate`
    -- a `block_residential_developer_estimate`/`_simulate` step pair
    would read this format directly.

    Args:
        final_model: The `final_model` dict from `run_kfold_cv` (fit on the
            full dataset).
        spec_name: Spec identifier (e.g. `"baseline"`), used in the yaml's
            `name` field.
        tract_validation: Optional dict from `compute_tract_validation`; if
            given, its `propensity_stats` is written under a
            `tract_validation` key.
        path: Destination yaml path.

    Returns:
        The dict that was written to `path`.

    Example:
        >>> spec = export_spec_yaml(cv_result['final_model'], 'baseline', tract_validation)
        >>> spec['model_type']
        'binary_discretechoice'

    See Also:
        train_block_developer: calls this after fitting and validating a spec.
    """
    summary_df = final_model['summary_df'].set_index('Variable')
    fit_parameters = {
        'Coefficient': {k: float(v) for k, v in summary_df['Coefficient'].items()},
        'Std. Error': {k: float(v) for k, v in summary_df['Std Error'].items()},
        'T-Score': {k: float(v) for k, v in summary_df['z-value'].items()},
    }

    spec = {
        'name': f'block_residential_developer_{spec_name}',
        'model_type': 'binary_discretechoice',
        'model_expression': ' + '.join(final_model['model_features']),
        'fitted': True,
        'fit_parameters': fit_parameters,
        'fit_diagnostics': {
            'train_auc': float(final_model['train_auc']),
            'test_auc': float(final_model['test_auc']),
            'log_likelihood': float(final_model['glm_result'].llf),
            'aic': float(final_model['glm_result'].aic),
            'bic': float(final_model['glm_result'].bic_llf),
        },
    }
    if tract_validation is not None:
        spec['tract_validation'] = {
            'propensity': {k: float(v) for k, v in tract_validation['propensity_stats'].items()},
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as yaml_file:
        yaml.safe_dump(spec, yaml_file, sort_keys=False, default_flow_style=False)
    print(f'Wrote fitted spec -> {path}')
    return spec


# ---------------------------------------------------------------------------- #
#                              Primary entry point                             #
# ---------------------------------------------------------------------------- #

def train_block_developer(
    spec_name='baseline',
    census_api_key=None,
    n_folds=N_FOLDS,
    random_state=KFOLD_RANDOM_STATE,
    spec_yaml_path=_SPEC_YAML_PATH,
):
    """Assembles the estimation dataset, fits, validates, and exports one spec.

    Orchestrates the full training pipeline for a single named spec in
    `BLOCK_DEVELOPER_SPECS`: builds the estimation frame, runs stratified
    k-fold cross-validation, validates the block-grain fit at tract grain, and
    writes the full-dataset fit to the spec yaml.

    Args:
        spec_name: Key into `BLOCK_DEVELOPER_SPECS` identifying which
            RHS variable list to fit.
        census_api_key: Census API key, passed through to
            `build_block_developer_dataset`. Required only on the first run,
            when the decennial caches are absent.
        n_folds: Number of cross-validation folds.
        random_state: Random seed for fold assignment reproducibility.
        spec_yaml_path: Destination yaml path for the fitted spec.

    Returns:
        A dict with keys `cv_result` (from `run_kfold_cv`) and
        `tract_validation` (from `compute_tract_validation`).

    Example:
        >>> result = train_block_developer(spec_name='baseline', census_api_key='YOUR_KEY')
        >>> result['tract_validation']['propensity_stats']['r2'] > 0
        True

    See Also:
        compare_specs: fits and tabulates every spec in a registry at once,
            useful before choosing which spec to pass here.
    """
    estimation_frame = build_block_developer_dataset(census_api_key=census_api_key)
    frame = prepare_block_estimation_frame(estimation_frame)
    fold_indices = build_fold_indices(frame, n_folds=n_folds, random_state=random_state)

    feature_vars = BLOCK_DEVELOPER_SPECS[spec_name]
    cv_result = run_kfold_cv(feature_vars, spec_name, frame, fold_indices)
    tract_validation = compute_tract_validation(frame, cv_result['oof_probs'])

    print(f"\nTract-level validation ({spec_name}):")
    print(f"  Propensity: slope={tract_validation['propensity_stats']['slope']:.3f}  "
          f"R2={tract_validation['propensity_stats']['r2']:.3f}  "
          f"RMSE={tract_validation['propensity_stats']['rmse']:.2f}")

    export_spec_yaml(cv_result['final_model'], spec_name, tract_validation, spec_yaml_path)

    return dict(cv_result=cv_result, tract_validation=tract_validation)


if __name__ == '__main__':
    train_block_developer(census_api_key=os.getenv('CENSUS_API_KEY'))
