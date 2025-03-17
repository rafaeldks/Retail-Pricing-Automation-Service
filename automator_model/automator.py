import logging
from enum import Enum
import pandas as pd
import numpy as np
from numpy.core.defchararray import lower
from tabulate import tabulate
from typing import List, Optional, Dict, Type

from src.price_round import PriceRounder
from src.utils.logger_config import logger
from src.automator.loader import get_default_price_rounder
from src.utils.utils import log_execution_time, is_null, not_null
from src.automator.strategies import (
    PriceResult,
    BaseStrategy,
    CurrentPriceStrategy,
    BaseMarginStrategy,
    MinPriceStrategy,
    CompetitorStrategy,
    PriorityCompetitorsStrategy,
)


DEFAULT_PRIORITY_COMPETITORS_LIST = ['competitor_1', 'competitor_2', 'competitor_3']

class PricingStrategy(str, Enum):
    PRIORITY_COMPETITORS = "Priority Competitors"
    BASE_MARGIN = "Base Margin"
    CURRENT_PRICE = "Current Price"
    MINIMUM_PRICE = "Minimum Price"
    COMPETITOR = "Competitor"

    def __str__(self):
        return self.value

    @classmethod
    def from_str(cls, value, default=None):
        try:
            return cls(value)
        except ValueError:
            return default


class PricingAutomator:
    """
    A class to automate price calculation for products based on predetermined pricing strategies and commercial data.

    Strategies:
    - Current price: keep the price unchanged
    - Base margin: suggest a price based on a given margin level
    - Competitor: set the price equal to that of a specific competitor
    - Minimum price: set the minimum price among competitors
    - Priority competitors: choose the price from the most prioritized competitor

    The final price is determined after a series of preprocessing and price calculation logic
    with adjustments within a defined range and price alignment within product lines.

    Attributes:
        data (pd.DataFrame): Input data table
        base_tree, lower_tree, upper_tree (Dict): Fallback strategy trees
    """

    base_tree = {
        PriorityCompetitorsStrategy: [
            PriorityCompetitorsStrategy(DEFAULT_PRIORITY_COMPETITORS_LIST),
            BaseMarginStrategy('base_margin'),
            CurrentPriceStrategy(),
        ],
        CompetitorStrategy: [
            CompetitorStrategy('base_competitor'),
            BaseMarginStrategy('base_margin'),
            CurrentPriceStrategy(),
        ],
        MinPriceStrategy: [
            MinPriceStrategy(),
            BaseMarginStrategy('base_margin'),
            CurrentPriceStrategy(),
        ],
        BaseMarginStrategy: [
            BaseMarginStrategy('base_margin'),
            CurrentPriceStrategy(),
        ],
        CurrentPriceStrategy: [
            CurrentPriceStrategy(),
        ],
    }

    lower_tree = {
        PriorityCompetitorsStrategy: [PriorityCompetitorsStrategy(DEFAULT_PRIORITY_COMPETITORS_LIST)],
        CompetitorStrategy: [CompetitorStrategy('lower_competitor')],
        MinPriceStrategy: [MinPriceStrategy()],
        BaseMarginStrategy: [BaseMarginStrategy('lower_base_margin')],
        CurrentPriceStrategy: [CurrentPriceStrategy()],
    }

    upper_tree = {
        PriorityCompetitorsStrategy: [PriorityCompetitorsStrategy(DEFAULT_PRIORITY_COMPETITORS_LIST)],
        CompetitorStrategy: [CompetitorStrategy('upper_competitor')],
        MinPriceStrategy: [MinPriceStrategy()],
        BaseMarginStrategy: [BaseMarginStrategy('upper_base_margin')],
        CurrentPriceStrategy: [CurrentPriceStrategy()],
    }

    def __init__(
        self,
        data: Optional[pd.DataFrame] = None,
        priority_competitors_list: Optional[List[str]] = None,
        price_rounder: Optional[PriceRounder] = None,
        use_price_rounder: bool = False,
        competitors_line_removal_limit: int = -1,
        agg_line_price_only_where_existed: bool = False,
        fm_sensitivity_mode: str = "abs",
        competitors_fm_filter_threshold: float = 0.05,
        competitors_price_filter_threshold: float = -1,
        upper_margin_threshold: float = 0.05,
        lower_margin_threshold: float = 0.05,
        complex_target_competitors_selection: bool = False,
        strategies_sensitivity_mode: str = 'abs',
        use_preprocess_lines: bool = False,
        use_strategies_from_source: bool = False,
    ):
        if priority_competitors_list is None:
            self.priority_competitors_list = DEFAULT_PRIORITY_COMPETITORS_LIST
        else:
            self.priority_competitors_list = priority_competitors_list

        self.competitors_line_removal_limit = competitors_line_removal_limit
        self.agg_line_price_only_where_existed = agg_line_price_only_where_existed
        self.fm_sensitivity_mode = fm_sensitivity_mode
        self.competitors_fm_filter_threshold = competitors_fm_filter_threshold
        self.competitors_price_filter_threshold = competitors_price_filter_threshold
        self.upper_margin_threshold = upper_margin_threshold
        self.lower_margin_threshold = lower_margin_threshold
        self.complex_target_competitors_selection = complex_target_competitors_selection
        self.strategies_sensitivity_mode = strategies_sensitivity_mode
        self.use_preprocess_lines = use_preprocess_lines
        self.use_strategies_from_source = use_strategies_from_source

        self.merged_data = data

        self.use_price_rounder = use_price_rounder
        self.price_rounder = None
        if use_price_rounder:
            self.price_rounder = price_rounder if price_rounder else get_default_price_rounder()

        self.line_competitor_price_dict = {}

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("PricingAutomator initialized with data shape: %s, columns: %s",
                         self.merged_data.shape, self.merged_data.columns.tolist())

    @log_execution_time
    def preprocess_data(self):
        """
        Preprocesses the input data.
        """
        # Your code for preprocessing here

    def compute_fm_given_price(self, price, cost):
        """
        Calculate the front margin (fm) given a price and cost.

        fm = (price - cost) / price

        Args:
            price (float): The price
            cost (float): The cost including VAT

        Returns:
            float or np.nan: fm or np.nan if the price is invalid
        """
        if price is None or pd.isna(price) or price == 0:
            return np.nan
        return (price - cost) / price

    @log_execution_time
    def preprocess_lines(self, use_custom_lines: bool = False, group_cols: Optional[List[str]] = None, x: float = 0.05):
        """
        Preprocesses product lines using custom or default grouping columns and reseting purchase prices to the mode value.

        Args:
            use_custom_lines (bool): Flag to use custom grouping columns
            group_cols (List[str]): Grouping columns
            x (float): Tolerance for deviation from mode
        """
        # Your code for line preprocessing here

    @log_execution_time
    def preprocess_strategies(self, target_competitors: Optional[List[str]] = None):
        """
        Preprocesses pricing strategies for each product, setting base, upper, and lower strategies based on conditions.

        Args:
            target_competitors (Optional[List[str]]): List of target competitors
        """
        # Your code for strategies preprocessing here

    @log_execution_time
    def preprocess_competitors(self, x: float = 0.1, y: float = 0.02):
        """
        Filters out competitors based on front margin comparison and sales data.

        Args:
            x (float): Margin sensitivity threshold
            y (float): Price sensitivity threshold
        """
        # Your code for competitors preprocessing here

    def _get_line_competitor_price(self, row: pd.Series, competitor_name: str) -> Optional[float]:
        """
        Retrieve aggregated line-level competitor price.

        Args:
            row (pd.Series): The data row
            competitor_name (str): The competitor's name

        Returns:
            Optional[float]: Aggregated price if available
        """
        # Your code for getting line competitor price here

    @log_execution_time
    def aggregate_line_competitor_prices(self):
        """
        Aggregates competitor prices across product lines.
        """
        # Your code for aggregating line competitor prices here

    def calculate_new_price(
        self,
        row: pd.Series,
        strategy_list: List[BaseStrategy],
    ) -> Optional[PriceResult]:
        """
        Calculate a new price for a product based on a priority list of strategies.

        Args:
            row (pd.Series): Data row with product information
            strategy_list (List[BaseStrategy]): List of strategies in order of priority

        Returns:
            Optional[PriceResult]: Calculated price and strategy details
        """
        for strategy in strategy_list:
            if strategy:
                result = strategy.compute(row)
                if result and not_null(result.price):
                    return result

        return None

    @log_execution_time
    def compute_individual_prices(
        self,
        strategy_col: str,
        new_price_col: str,
        tree: Dict[Type[BaseStrategy], List[BaseStrategy]],
    ):
        """
        Computes new prices for all products based on their respective strategies.

        Args:
            strategy_col (str): Name of the column with strategies.
            new_price_col (str): Name of the column to store results.
            tree (Dict[PricingStrategy, PricingStrategy[str]]): Strategy priority tree.
        """
        # Your code for computing individual prices here

    @log_execution_time
    def determine_line_prices(self, price_col: str, price_dict_attr: str):
        """
        Determine the highest price for a product line in each city.

        Args:
            price_col (str): Column name with prices.
            price_dict_attr (str): Attribute name to store price dictionary.
        """
        # Your code for determining line prices here

    @log_execution_time
    def assign_line_prices(self, price_col: str, price_dict_attr: str):
        """
        Assigns a uniform price to all products in the same line in a city.

        Args:
            price_col (str): Column name with prices.
            price_dict_attr (str): Attribute with price dictionary.
        """
        # Your code for assigning line prices here

    @log_execution_time
    def round_price(self, price_col: str):
        """
        Rounds the values in the specified price column.

        Args:
            price_col (str): Name of the column to round.
        """
        dtype = self.merged_data[price_col].dtype
        self.merged_data[price_col] = self.merged_data[price_col].apply(self._round_value).astype(dtype)

    def _round_value(self, x):
        """
        Rounds a value based on self.use_price_rounder.

        Args:
            x (float): Value to round.

        Returns:
            float: Rounded value.
        """
        if is_null(x):
            return np.nan

        if self.use_price_rounder:
            return self.price_rounder.get_rounded_price(x)
        else:
            return round(x)

    @log_execution_time
    def add_metrics(self, price_column='new_price_final', label='new'):
        """
        Adds metrics such as GMV, Front Margin, and Front Margin (%) based on the prices in the given column.

        Args:
            price_column (str): Column name with prices for metric calculation.
            label (str): Suffix label for metric columns.
        """
        # Your code for adding metrics here

    @log_execution_time
    def compute_metrics(self, top_n: int = 5, group_cols: Optional[List[str]] = None):
        """
        Compute and log metrics for price changes, highlighting top changes.

        Args:
            group_cols (List[str]): Columns for grouping metrics.
            top_n (int): Number of top changes to log.
        """
        # Your code for computing metrics here

    @log_execution_time
    def build_reason_column(self):
        """
        Constructs a "reason" column that provides a business explanation of the final price.
        """
        # Your code for building reason column here

    @log_execution_time
    def run(self) -> pd.DataFrame:
        """
        Executes the full pipeline for data processing and price calculation.

        Returns:
            pd.DataFrame: Updated DataFrame with final prices.
        """
        self.preprocess_data()
        self.compute_individual_prices(
            'base_strategy',
            'new_price_base',
            self.base_tree
        )

        self.merged_data['new_price_lower'] = None
        self.merged_data['new_price_upper'] = None

        self.compute_individual_prices(
            'lower_strategy',
            'new_price_lower',
            tree=self.lower_tree
        )

        self.compute_individual_prices(
            'upper_strategy',
            'new_price_upper',
            tree=self.upper_tree
        )

        self.merged_data['new_price_final'] = self.merged_data['new_price_base'].clip(
            lower=self.merged_data['new_price_lower'],
            upper=self.merged_data['new_price_upper']
        )
        self.merged_data['price_after_clip'] = self.merged_data['new_price_final']
        self.round_price('new_price_final')

        self.merged_data['price_after_rounding'] = self.merged_data['new_price_final']
        self.determine_line_prices('new_price_final', 'line_price_final_dict')
        self.assign_line_prices('new_price_final', 'line_price_final_dict')

        self.compute_metrics()
        self.build_reason_column()

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Final data head after run:\n%s", self.merged_data.head())

        return self.merged_data
