from __future__ import annotations
from typing import NamedTuple, Optional, List
from abc import ABC, abstractmethod
import pandas as pd

from src.utils.utils import not_null

class BaseStrategy(ABC):
    """
    Abstract base class for all pricing strategies.
    All strategies should have an attribute `name` and implement the `compute` method.
    """
    name: str

    @abstractmethod
    def compute(self, row: pd.Series) -> Optional[PriceResult]:
        pass

    def __str__(self):
        # Useful for debugging.
        return self.name

    def __eq__(self, other):
        # For testing.
        if not isinstance(other, BaseStrategy):
            return NotImplemented
        # Compare dictionaries of all attributes.
        return self.__dict__ == other.__dict__

    def __hash__(self):
        # Generate hash based on all object attributes.
        return hash(tuple((key, value) for key, value in self.__dict__.items()))

class PriceResult(NamedTuple):
    """
    Result of a pricing strategy calculation:
    - price: the resulting calculated price (Optional[float])
    - strategy: name of the strategy (Optional[BaseStrategy])
    - description: business description of the calculation result (Optional[str])
    """
    price: Optional[float]
    strategy: Optional[BaseStrategy]
    description: Optional[str]

    def as_dict(self):
        return self._asdict()

class CurrentPriceStrategy(BaseStrategy):
    """
    Returns the current price.
    """
    name = "Current Price"

    def compute(self, row: pd.Series) -> Optional[PriceResult]:
        current = row.get('current_price', None)
        if pd.notna(current):
            description = f"Used current price: {current}"
            return PriceResult(price=current, strategy=self, description=description)
        return None

class BaseMarginStrategy(BaseStrategy):
    """
    Calculates the price based on base margin:
    price = cost / (1 - desired_margin)
    """
    name = "Base Margin"

    def __init__(self, margin_col: str):
        self.margin_col = margin_col

    def compute(self, row: pd.Series) -> Optional[PriceResult]:
        margin = row.get(self.margin_col)
        purchase_price = row.get('purchase_price')
        vat = row.get('vat')
        if not_null(margin) and not_null(purchase_price) and not_null(vat):
            cost = purchase_price + vat
            if (1 - margin) != 0:
                new_price = cost / (1 - margin)
                description = (
                    f"Calculated price using base margin: {cost:.1f} / (1 - {margin:.2f}) = {new_price:.1f}"
                )
                return PriceResult(price=new_price, strategy=self, description=description)
        return None

class MinPriceStrategy(BaseStrategy):
    """
    Returns the lowest available competitor price, if any.
    """
    name = "Minimum Price"

    def compute(self, row: pd.Series) -> Optional[PriceResult]:
        all_comps = row.get('all_competitors', {})
        if isinstance(all_comps, dict) and all_comps:
            comp_name, comp_price = min(all_comps.items(), key=lambda x: x[1])
            description = f"Used competitor {comp_name} with the lowest price: {comp_price:.1f}"
            return PriceResult(price=comp_price, strategy=self, description=description)
        return None

class CompetitorStrategy(BaseStrategy):
    """
    Returns the price of a specified competitor, if available.
    """
    name = "Competitor Price"

    def __init__(self, competitor_col: str):
        self.competitor_col = competitor_col

    def compute(self, row: pd.Series) -> Optional[PriceResult]:
        competitor = row.get(self.competitor_col, None)
        if competitor:
            comp_price = row.get('all_competitors', {}).get(competitor, None)
            if comp_price is not None:
                description = f"Used price {comp_price:.1f} for selected competitor {competitor}"
                return PriceResult(price=comp_price, strategy=self, description=description)
        return None

class PriorityCompetitorsStrategy(BaseStrategy):
    """
    Goes through the priority competitors list and picks the first available price.
    """
    name = "Priority Competitors"

    def __init__(self, default_priority_list: List[str]):
        self.default_priority_list = default_priority_list

    def compute(self, row: pd.Series) -> Optional[PriceResult]:
        all_comps = row.get('all_competitors', {})
        priority_list = row.get('priority_competitors', self.default_priority_list)
        for competitor in priority_list:
            if competitor in all_comps:
                comp_price = all_comps[competitor]
                description = f"Used price {comp_price:.1f} from the most prioritized competitor {competitor}"
                return PriceResult(price=comp_price, strategy=self, description=description)
        return None