# -*- coding: utf-8 -*-
"""
File: code_for_project_1
Created on 3/25/2026 5:52 PM

Description:


@author: zzzrrr
@email: zr15255082001@gmail.com/rz2759@columbia.edu
@version: 1.0
"""

import re
import shutil
from pathlib import Path

import pandas as pd

MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = MODULE_DIR / "datasets"
DEFAULT_RESULT_DIR = MODULE_DIR / "result"
EXCEL_FILENAME = "Data for final project 1.xlsx"
INDEX_RETURNS_SHEET = "Index returns in USD"
MARKET_VALUES_SHEET = "Market values in USD"
INDEX_RETURNS_CSV = "index_return.csv"
MARKET_VALUES_CSV = "market_value.csv"


def get_data(refresh_csv: bool = False) ->tuple[pd.DataFrame, pd.DataFrame]:
    """Load the project data. You don't need to pass data_dir."""
    data_dir_path = DEFAULT_DATA_DIR
    excel_path = data_dir_path / EXCEL_FILENAME
    index_return_csv = data_dir_path / INDEX_RETURNS_CSV
    market_value_csv = data_dir_path / MARKET_VALUES_CSV

    if refresh_csv or not (
            index_return_csv.exists() and market_value_csv.exists()
    ):
        if not excel_path.exists():
            raise FileNotFoundError(
                f"Put {EXCEL_FILENAME} in {data_dir_path}"
            )
        data_dir_path.mkdir(parents=True, exist_ok=True)
        sheets = pd.read_excel(
            excel_path,
            sheet_name=[INDEX_RETURNS_SHEET, MARKET_VALUES_SHEET],
        )
        sheets[INDEX_RETURNS_SHEET].to_csv(index_return_csv, index=False)
        sheets[MARKET_VALUES_SHEET].to_csv(market_value_csv, index=False)

    index_return_df = pd.read_csv(index_return_csv)
    market_value_df = pd.read_csv(market_value_csv)
    return index_return_df, market_value_df


def _sanitize_name(name: str) -> str:
    """Convert a label into a filesystem-safe stem."""
    clean_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    clean_name = clean_name.strip("._")
    if not clean_name:
        raise ValueError("Name must contain at least one valid character.")
    return clean_name


def get_result_dir(create: bool = True) -> Path:
    """Return the default result directory next to datasets."""
    if create:
        DEFAULT_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_RESULT_DIR


def get_strategy_result_dir(
        run_name: str | None,
        create: bool = True,
        clear_existing: bool = False,
) -> Path:
    """Return the result directory for one named strategy."""
    if run_name is None or not run_name.strip():
        raise ValueError(
            "run_name is required. Pass a descriptive result name such as "
            "'bond_em' or 'inflation_em_value'."
        )

    strategy_dir = get_result_dir(create=create) / _sanitize_name(run_name)
    if clear_existing and strategy_dir.exists():
        shutil.rmtree(strategy_dir)
    if create:
        strategy_dir.mkdir(parents=True, exist_ok=True)
    return strategy_dir


def _save_dataframe_group(
        *,
        strategy_dir: Path,
        group_name: str,
        dataframes: dict[str, pd.DataFrame],
        saved_paths: list[dict[str, str]],
) -> None:
    """Save one dataframe group under result/<strategy>/<group>/."""
    group_dir = strategy_dir / _sanitize_name(group_name)
    group_dir.mkdir(parents=True, exist_ok=True)

    for label, df in dataframes.items():
        artifact_name = _sanitize_name(label)
        artifact_path = group_dir / f"{artifact_name}.csv"
        df.to_csv(artifact_path, index=False)
        saved_paths.append(
            {
                "Group": group_name,
                "Artifact": label,
                "Path": str(artifact_path),
            }
        )


def _save_figure_group(
        *,
        strategy_dir: Path,
        group_name: str,
        figures: dict[str, object],
        image_format: str,
        saved_paths: list[dict[str, str]],
) -> None:
    """Save one figure group under result/<strategy>/<group>/."""
    group_dir = strategy_dir / _sanitize_name(group_name)
    group_dir.mkdir(parents=True, exist_ok=True)

    image_suffix = _sanitize_name(image_format).lower()
    for label, figure in figures.items():
        artifact_name = _sanitize_name(label)
        artifact_path = group_dir / f"{artifact_name}.{image_suffix}"
        figure.savefig(artifact_path, dpi=200, bbox_inches="tight")
        saved_paths.append(
            {
                "Group": group_name,
                "Artifact": label,
                "Path": str(artifact_path),
            }
        )


def save_grouped_results(
        *,
        enabled: bool,
        run_name: str | None,
        dataframe_groups: dict[str, dict[str, pd.DataFrame]] | None = None,
        figure_groups: dict[str, dict[str, object]] | None = None,
        image_format: str = "png",
        clear_existing_strategy_dir: bool = True,
) -> pd.DataFrame | None:
    """
    Save grouped outputs under result/<run_name>/.

    The same run_name overwrites the previous strategy folder by clearing it
    first, so the directory always reflects the latest version of that strategy.
    """
    if not enabled:
        return None

    strategy_dir = get_strategy_result_dir(
        run_name=run_name,
        create=True,
        clear_existing=clear_existing_strategy_dir,
    )

    saved_paths: list[dict[str, str]] = []

    for group_name, dataframes in (dataframe_groups or {}).items():
        _save_dataframe_group(
            strategy_dir=strategy_dir,
            group_name=group_name,
            dataframes=dataframes,
            saved_paths=saved_paths,
        )

    for group_name, figures in (figure_groups or {}).items():
        _save_figure_group(
            strategy_dir=strategy_dir,
            group_name=group_name,
            figures=figures,
            image_format=image_format,
            saved_paths=saved_paths,
        )

    return pd.DataFrame(saved_paths)


def save_experiment_outputs(
        *,
        enabled: bool,
        run_name: str | None,
        backtest_results: pd.DataFrame,
        optimal_weights: pd.DataFrame,
        optimization_diagnostics: pd.DataFrame,
        holding_period_summary: pd.DataFrame,
        adaptive_view_summary: pd.DataFrame,
        q_table: pd.DataFrame,
        omega_multiplier_table: pd.DataFrame,
        summary_df: pd.DataFrame,
        growth_fig: object,
        relative_fig: object,
        table_fig: object,
        image_format: str = "png",
) -> pd.DataFrame | None:
    """
    Save the standard experiment outputs for this project.

    Design choices:
    - If `enabled` is False, do nothing and return None.
    - If `enabled` is True and `run_name` is missing, raise a clear error.
    - Results are always saved under `result/<run_name>/` next to this module,
      which is the same folder level as the notebook in this project.
    - Reusing the same `run_name` clears that strategy directory first, so the
      latest outputs replace the old ones cleanly.
    """
    return save_grouped_results(
        enabled=enabled,
        run_name=run_name,
        dataframe_groups={
            "bl": {
                "adaptive_view_summary": adaptive_view_summary,
                "q_table": q_table.reset_index(),
                "omega_multiplier_table": omega_multiplier_table.reset_index(),
                "optimal_weights": optimal_weights,
                "optimization_diagnostics": optimization_diagnostics,
            },
            "backtest": {
                "backtest_results": backtest_results,
                "holding_period_summary": holding_period_summary,
            },
            "summary": {
                "performance_summary": summary_df,
            },
        },
        figure_groups={
            "figures": {
                "growth_chart": growth_fig,
                "relative_outperformance_chart": relative_fig,
                "performance_summary_table": table_fig,
            },
        },
        image_format=image_format,
    )
