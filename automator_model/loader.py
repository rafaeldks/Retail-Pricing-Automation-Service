import pandas as pd
import numpy as np
import json
from ast import literal_eval
from typing import Optional
from os.path import join as join_path
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.price_round import PriceRounder
from src.utils.logger_config import logger
from src.yt_db import YtClient
from src.utils.utils import is_null

class DataLoader:
    """
    Handles loading and merging of various data sources required for price automation.
    """

    DATA_PATHS = {
        'pricing_strategies': '/path/to/pricing_strategies',
        'active_items': '/path/to/active_items',
        'snapshots': '/path/to/snapshots',
        'purchase_prices': '/path/to/purchase_prices',
        'products': '/path/to/products',
        'lines': '/path/to/lines',
        'commercial_metrics': '/path/to/commercial_metrics',
        'price_lists_product': '/path/to/price_lists_product',
        'price_lists': '/path/to/price_lists',
        'stores': '/path/to/stores',
        'price_rounding': '/path/to/price_rounding',
        'priority_competitors': '/path/to/priority_competitors'
    }

    COMM_METRICS_DEPTH_DAYS = 30

    def __init__(self, on_date: Optional[pd.Timestamp] = None):
        self.on_date = on_date or pd.Timestamp.today().floor(freq='D')
        self.on_date_str = self.on_date.strftime("%Y-%m-%d")
        self.yt_client = YtClient()
        self.data = None
        self.pricing_strategies = None
        self.competitor_prices = None
        self.active_items = None
        self.costs = None
        self.products = None
        self.lines = None
        self.comm_metrics = None
        self.current_prices = None
        self.price_lists_data = None
        self.price_rounding = None
        self.priority_competitors = None

    def log_uniqueness(self, df: pd.DataFrame, keys: list, df_name: str):
        unique_count = df.drop_duplicates(subset=keys).shape[0]
        total_count = df.shape[0]
        if unique_count != total_count:
            logger.warning(f"Duplicates in {df_name}: {total_count - unique_count} duplicates for {keys}")

    @staticmethod
    def convert_to_float(s):
        try:
            return float(s[:-1]) / 100
        except (ValueError, TypeError):
            return np.nan

    def load_pricing_strategies(self):
        columns = [
            'region', 'product_id', 'base_margin', 'base_competitor', 'strategy',
            'margin_lower', 'competitor_lower', 'strategy_lower',
            'margin_upper', 'competitor_upper', 'strategy_upper'
        ]
        max_path = join_path(self.DATA_PATHS['snapshots'], self.on_date_str)
        last_path = self.yt_client.get_last_table_in_directory(self.DATA_PATHS['snapshots'], max_path)
        df = self.yt_client.download_table(last_path)[columns].drop_duplicates(['region', 'product_id'])
        df['base_margin'] = df['base_margin'].fillna(df['margin_upper']).fillna(df['margin_lower'])
        df['base_margin'] = df['base_margin'].apply(self.convert_to_float)
        df[['margin_upper', 'margin_lower']] = df[['margin_upper', 'margin_lower']].applymap(self.convert_to_float)
        df['product_id'] = df['product_id'].astype(str)
        self.pricing_strategies = df
        self.log_uniqueness(self.pricing_strategies, ['region', 'product_id'], 'pricing_strategies')

    def process_competitors(self, use_price_w_promo: bool = False):
        def adjust_price(row):
            prices = json.loads(row["comp_prices"].replace("'", '"'))
            return {k.replace("_original", "").replace("_promo", ""): v * (1 + (row.get(f"{k}_coef", 0))) for k, v in prices.items() if use_price_w_promo == ("_promo" in k)}

        self.competitor_prices['all_competitors'] = self.competitor_prices.apply(adjust_price, axis=1)

    def load_competitor_prices(self):
        max_path = join_path(self.DATA_PATHS['snapshots'], self.on_date_str)
        last_path = self.yt_client.get_last_table_in_directory(self.DATA_PATHS['snapshots'], max_path)
        query = f"SELECT region, CAST(product_id AS string) AS product_id, comp_prices FROM `{last_path}`"
        competitor_prices = self.yt_client.download_data(query)
        self.competitor_prices = competitor_prices.drop_duplicates(['region', 'product_id'])
        self.process_competitors()
        self.competitor_prices['snapshot_date'] = last_path.split('/')[-1]
        self.log_uniqueness(self.competitor_prices, ['region', 'product_id'], 'competitor_prices')

    def load_active_items(self):
        self.active_items = self.yt_client.download_table(self.DATA_PATHS['active_items'])[['region', 'product_id']]
        self.active_items['product_id'] = self.active_items['product_id'].astype(str)
        self.log_uniqueness(self.active_items, ['region', 'product_id'], 'active_items')

    def load_costs(self):
        max_path = join_path(self.DATA_PATHS['snapshots'], self.on_date_str)
        last_path = self.yt_client.get_last_table_in_directory(self.DATA_PATHS['snapshots'], max_path)
        query = f"SELECT region, CAST(product_id AS String) AS product_id, purchase_price_wo_vat * vat AS vat_in, purchase_price_wo_vat AS purchase_price FROM `{last_path}`"
        self.costs = self.yt_client.download_data(query).drop_duplicates(['region', 'product_id'])

    def load_costs_from_replica(self):
        costs = self.yt_client.download_table(self.DATA_PATHS['purchase_prices'])
        to_float = lambda x: float(x.replace("\xa0", "").replace(" ", "").replace(',', '.') if x else 'nan')
        costs['purchase_price'] = costs['price'].apply(to_float)
        costs['vat_in'] = costs['vat'].apply(to_float)
        costs = costs.rename(columns={'product_id': 'product_id'})[['product_id', 'region', 'purchase_price', 'vat_in']]
        self.costs = costs.groupby(['region', 'product_id'], as_index=False).agg({'purchase_price': 'max', 'vat_in': 'max'})
        self.log_uniqueness(self.costs, ['region', 'product_id'], 'costs')

    def load_products(self):
        self.products = self.yt_client.download_table(self.DATA_PATHS['products'])[[
            'product_id', 'brand', 'weight_gross', 'category',
            'prepared_food', 'private_label', 'vat_out'
        ]]
        self.products = self.products.astype({
            'prepared_food': bool,
            'private_label': bool
        })
        self.log_uniqueness(self.products, ['product_id'], 'products')

    def load_lines(self):
        max_path = join_path(self.DATA_PATHS['snapshots'], self.on_date_str)
        path = self.yt_client.get_last_table_in_directory(self.DATA_PATHS['snapshots'], max_path)
        query = f"SELECT region, CAST(product_id AS String) AS product_id, line FROM `{path}`"
        self.lines = self.yt_client.download_data(query).drop_duplicates(['region', 'product_id'])
        self.log_uniqueness(self.lines, ['region', 'product_id'], 'lines')

    def load_commercial_metrics(self):
        start_dt = self.on_date - pd.Timedelta(days=self.COMM_METRICS_DEPTH_DAYS)
        start_dt_str = start_dt.strftime("%Y-%m-%d")
        query = f"SELECT region, product_id, SUM(sold_qty) AS sales FROM `{self.DATA_PATHS['commercial_metrics']}` WHERE date BETWEEN '{start_dt_str}' AND '{self.on_date_str}' GROUP BY region, product_id"
        self.comm_metrics = self.yt_client.download_data(query)
        self.log_uniqueness(self.comm_metrics, ['region', 'product_id'], 'comm_metrics')

    def load_current_prices(self):
        query = f"SELECT product_id, price_list_id, price_w_vat AS current_price FROM `{self.DATA_PATHS['price_lists_product']}`"
        self.current_prices = self.yt_client.download_data(query)
        self.log_uniqueness(self.current_prices, ['product_id', 'price_list_id'], 'current_prices')

    def load_price_lists_data(self):
        query = f"SELECT region, price_list_id FROM `{self.DATA_PATHS['stores']}` AS st JOIN `{self.DATA_PATHS['price_lists']}` AS pl USING (price_list_id) WHERE pl.name LIKE '%_MAIN' GROUP BY region, price_list_id"
        self.price_lists_data = self.yt_client.download_data(query)
        self.log_uniqueness(self.price_lists_data, ['region'], 'price_lists_data')

    def load_price_rounding(self):
        self.price_rounding = self.yt_client.download_table(self.DATA_PATHS['price_rounding'])[['left_bound', 'right_bound', 'rounded_price']]
        self.log_uniqueness(self.price_rounding, ['left_bound', 'right_bound'], 'price_rounding')

    def load_priority_competitors(self):
        df = self.yt_client.download_table(self.DATA_PATHS['priority_competitors'])
        competitor_cols = [f'competitor_{i}' for i in range(1, len(df.columns))]
        df['priority_competitors'] = df[competitor_cols].agg(lambda row: [x for x in row if pd.notna(x)], axis=1)
        self.priority_competitors = df[['region', 'priority_competitors']]

    def collect_all_data(self):
        logger.info('Starting data loading')

        load_methods = [
            self.load_pricing_strategies,
            self.load_competitor_prices,
            self.load_active_items,
            self.load_costs,
            self.load_products,
            self.load_lines,
            self.load_commercial_metrics,
            self.load_current_prices,
            self.load_price_lists_data,
            self.load_price_rounding,
            self.load_priority_competitors,
        ]

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(method): method.__name__ for method in load_methods}

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    method_name = futures[future]
                    logger.error(f'Error in method {method_name}: {e}')
                    raise

        logger.info('Data loading completed')
        logger.info(f'pricing_strategies shape: {self.pricing_strategies.shape}')
        logger.info(f'competitor_prices shape: {self.competitor_prices.shape}')
        logger.info(f'active_items shape: {self.active_items.shape}')
        logger.info(f'costs shape: {self.costs.shape}')
        logger.info(f'products shape: {self.products.shape}')
        logger.info(f'lines shape: {self.lines.shape}')
        logger.info(f'comm_metrics shape: {self.comm_metrics.shape}')

    def merge_data(self):
        logger.info('Starting data merging')

        merged_data = pd.merge(
            self.active_items,
            self.pricing_strategies,
            on=['region', 'product_id'],
            how='left',
            validate='one_to_one'
        )
        merged_data = pd.merge(
            merged_data,
            self.competitor_prices,
            on=['region', 'product_id'],
            how='left',
            validate='one_to_one'
        )
        merged_data = pd.merge(merged_data, self.priority_competitors, on=['region'], how='left')
        merged_data = pd.merge(merged_data, self.costs, on=['region', 'product_id'], how='left', validate='one_to_one')
        merged_data = pd.merge(merged_data, self.products, on='product_id', how='left', validate='many_to_one')
        merged_data['vat'] = np.where(
            merged_data['vat_out'] > 0 & (
                (merged_data['vat_in'].isna()) | (merged_data['vat_in'] == 0)
            ),
            merged_data['purchase_price'] * merged_data['vat_out'] / 100,
            merged_data['vat_in']
        )
        merged_data = pd.merge(merged_data, self.lines, on=['region', 'product_id'], how='left', validate='one_to_one')
        merged_data = pd.merge(
            merged_data,
            self.comm_metrics,
            left_on=['region', 'product_id'],
            right_on=['region', 'product_id'],
            how='left',
            validate='one_to_one'
        )
        merged_data = merged_data.rename(columns={'vat_out': 'vat_outgoing_percentage'})
        merged_data['report_date'] = self.on_date_str
        self.log_uniqueness(merged_data, ['region', 'product_id'], 'merged_data')
        logger.info('Data merge complete')

        return merged_data

    def collect_and_merge_data(self):
        self.collect_all_data()
        return self.merge_data()


def get_default_price_rounder():
    data_loader = DataLoader()
    data_loader.load_price_rounding()
    return PriceRounder(data_loader.price_rounding, 'left_bound', 'right_bound', 'rounded_price')