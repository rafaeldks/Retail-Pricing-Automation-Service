$format_date = DateTime::Format('%Y-%m-%d');
$day_before = $format_date(CurrentUtcDate() - Interval('P1D'));
$date_30_days_ago = $format_date(CurrentUtcDate() - Interval('P30D'));
$start_of_p30d_month = SUBSTRING($date_30_days_ago, 0, 7) || '-01';
$excluded_categories = ('Category_1', 'Category_2', 'Category_3', 'Category_4', 'Category_5', 'Category_6');
$locations = ('City_Alpha', 'City_Beta', 'City_Gamma', 'City_Delta', 'City_Epsilon', 'City_Zeta');
$metrics_data_source = '//data/source/com/metric_daily';

$temp_price_list_purch_cost = (
    SELECT
        item_id,
        city,
        '0_source_data' AS source,
        max_by(cost_wo_vat, query_datetime) AS purchases_cost_wo_vat,
        max_by(supplier_name, query_datetime) AS supplier_id,
        max_by(cost_w_vat, query_datetime) AS purchases_cost_w_vat
    FROM
        range(`//data/analytics/pricing/regular_prices`)
    GROUP BY
        item_id,
        city
);

$temp_stores_excluding = (
    SELECT
        store_id,
        city_name_ru,
        (
            CASE
                WHEN cluster_info LIKE '%Cluster_X%' THEN 'Cluster_X'
                ELSE cluster_info
            END
        ) AS cluster_info
    FROM
        `//data/source/store/store`
    WHERE
        `region_name` == 'Region_Alpha'
        AND cluster_info != 'Central'
);

$temp_orders_purch_cost = (
    SELECT
        item_id,
        city_name,
        '1_orders' AS source,
        max_by(supplier_id, local_date) AS supplier_id,
        max_by(item_cost_w_o_vat_local, local_date) AS item_purchase_cost_wo_vat_lcy,
        max_by(item_cost_w_vat_local, local_date) AS item_purchase_cost_w_vat_lcy
    FROM (
        SELECT
            local_date,
            item_id,
            store_id,
            city_name,
            max_by(supplier_id, starting_item_count) AS supplier_id,
            max_by(item_cost_w_o_vat_local, starting_item_count) AS item_purchase_cost_wo_vat_lcy,
            max_by(item_cost_vat_local, starting_item_count) AS item_purchase_cost_vat_lcy,
            max_by(item_cost_w_vat_local, starting_item_count) AS item_purchase_cost_w_vat_lcy
        FROM (
            SELECT DISTINCT
                local_date,
                item_id,
                a.store_id AS store_id,
                city_name,
                supplier_name AS supplier_id,
                starting_item_count,
                item_cost_w_o_vat_local,
                item_cost_vat_local,
                item_cost_w_o_vat_local + item_cost_vat_local AS item_purchase_cost_w_vat_lcy
            FROM
                range($metrics_data_source, $start_of_p30d_month) AS a
            INNER JOIN
                $temp_stores_excluding AS s
            ON
                a.store_id == s.store_id
            WHERE
                1 == 1
                AND starting_item_count IS NOT NULL
                AND starting_item_count > 0
                AND city_name IN $locations
        )
        GROUP BY
            local_date,
            item_id,
            store_id,
            city_name
    )
    GROUP BY
        item_id,
        city_name AS city
);

$temp_purch_cost_aggregate = (
    SELECT
        item_id,
        city,
        min(source) AS source,
        min_by(supplier_id, source) AS supplier_id,
        MIN_BY(purchases_cost_wo_vat, source) AS purchases_cost_wo_vat,
        MIN_BY(purchases_cost_w_vat, source) AS purchases_cost_w_vat,
        max(if(source == '0_source_data', purchases_cost_wo_vat, NULL)) AS purchases_cost_wo_vat_pl,
        max(if(source == '1_orders', purchases_cost_wo_vat, NULL)) AS purchases_cost_wo_vat_orders,
    FROM (
        SELECT
            *
        FROM
            $temp_orders_purch_cost
        UNION ALL
        SELECT
            *
        FROM
            $temp_price_list_purch_cost
    )
    GROUP BY
        item_id,
        city
);

$temp_price_list_data = (
    SELECT
        locations.external_id AS place_id,
        locations.cluster AS city,
        locations.title AS store_name,
        price_lists.title AS price_lists_name,
        ConvertToString(
            ParseJson(
                ConvertToString(price_list_products.price)
            ).store
        ) AS catalog_price,
        products.external_id AS item_code,
        products.long_vendor_id AS product_name,
        products.product_id AS item_id_hash,
        products.vat AS output_tax,
        locations.store_id AS warehouse_id_hash
    FROM
        `//source/repo/wms/shops` AS locations
    JOIN
        `//source/repo/wms/pricing` AS price_lists
    ON
        locations.price_list_id == price_lists.price_list_id
    JOIN
        `//source/repo/wms/price_items` AS price_list_products
    ON
        price_list_products.price_list_id == locations.price_list_id
    JOIN
        `//source/repo/wms/products` AS products
    ON
        price_list_products.product_id == products.product_id
    JOIN (
        SELECT DISTINCT
            product_id,
            store_id
        FROM
            `//source/repo/wms/stocks`
        WHERE
            shelf_type == 'store'
    ) AS c
    ON
        products.product_id == c.product_id AND locations.store_id == c.store_id
);

$temp_prepared_price_list = (
    SELECT DISTINCT
        price_lists_name,
        CAST(place_id AS Int64) AS store_id,
        warehouse_id_hash,
        CAST(item_code AS Int64) AS item_id,
        item_id_hash,
        catalog_price AS retail_price_with_nds,
        city,
        product_name,
        output_tax,
    FROM
        $temp_price_list_data
    WHERE
        catalog_price IS NOT NULL
);

$temp_pre_final_data = (
    SELECT
        a.item_id AS item_id,
        a.price_lists_name AS price_list,
        a.city AS city,
        a.product_name AS product_name,
        CAST(a.retail_price_with_nds AS Double) AS retail_price_with_nds,
        a.store_id AS store_id,
        a.output_tax AS output_tax,
        CAST(purchases_cost_wo_vat AS Double) AS purchases_cost_wo_vat,
        CAST(purchases_cost_w_vat AS Double) AS purchases_cost_w_vat,
        b.supplier_id AS supplier_id,
        b.purchases_cost_wo_vat_pl AS purchases_cost_wo_vat_pl,
        b.purchases_cost_wo_vat_orders AS purchases_cost_wo_vat_orders,
        b.source AS source
    FROM
        $temp_prepared_price_list AS a
    LEFT JOIN
        $temp_purch_cost_aggregate AS b
    ON
        CAST(a.item_id AS String) == b.item_id
        AND CAST(a.city AS String) == b.city
);

$temp_final_aggregated = (
    SELECT
        item_id,
        price_list,
        city,
        product_name,
        ABS(1.0 * max(purchases_cost_wo_vat_pl - purchases_cost_wo_vat_orders) / avg(CAST(purchases_cost_wo_vat AS Double)) - 1) AS max_dif,
        count(DISTINCT Math::Round(purchases_cost_wo_vat)) AS number_of_different_prices,
        avg(CAST(retail_price_with_nds AS Double)) AS retail_price_with_nds,
        some(supplier_id) AS supplier_id,
        Math::Round(avg(CAST(purchases_cost_wo_vat AS Double)), -2) AS purchase_price_without_nds,
        Math::Round(avg(CAST(purchases_cost_w_vat AS Double)), -2) AS purchases_cost_with_nds,
        coalesce(Math::Round(CAST(avg(output_tax) AS float) / 100000000, -2), 0) AS output_tax
    FROM
        $temp_pre_final_data
    GROUP BY
        item_id,
        price_list,
        city,
        product_name
);

$temp_week_ago = $format_date(CurrentUtcDate() - Interval('P8D'));
$temp_week_ago_month = substring($temp_week_ago, 0, 7) || '-01';

$temp_pnl_by_city_data = (
    SELECT
        item_id,
        city,
        sum(COALESCE(cost_of_goods_sold_wo_tax_local, 0)) AS cogs,
        sum(COALESCE(margin_wo_tax_local, 0)) AS commercial_margin,
        sum(COALESCE(front_margin_wo_tax_local, 0)) AS front_margin_abs,
        sum(COALESCE(items_sold_count, 0)) AS sales_quantity,
        sum(COALESCE(revenue_wo_tax_local, 0)) AS gross_revenue,
        sum(COALESCE(writeoff_cost_wo_tax_local, 0)) AS writeoffs_cost,
        sum(COALESCE(gmv_with_tax_local, 0)) AS gmv,
        sum(COALESCE(total_discount_wo_tax_local, 0)) AS discount_size
    FROM
        range(`//source/dw/com/metric_data_daily`, $temp_week_ago_month)
    WHERE
        1 == 1
        AND local_date >= $temp_week_ago
        AND local_date <= $day_before
    GROUP BY
        CAST(item_id AS Int64) AS item_id,
        city_name AS city
);

$master_category_data = '//data/output/master_category/dim_records';

$temp_product_categories = (
    SELECT DISTINCT
        item_id AS item_code,
        lvl3_category AS category_name,
        lvl4_subcategory AS subcategory_name
    FROM
        $master_category_data
);

$temp_correct_comp_prices = '//data/analytics/pricing/correct_price_data';

$temp_max_dt_comp = (
    SELECT
        competitor_name,
        item_monitored_id,
        city,
        max(comp_date) AS dt_max
    FROM
        `$temp_correct_comp_prices`
    GROUP BY
        competitor_name,
        item_monitored_id,
        city
);

$temp_comp_price_by_competitors = (
    SELECT DISTINCT
        source,
        a.competitor_name AS competitor_name,
        CAST(a.item_monitored_id AS int) AS item_id,
        a.city AS city,
        asTuple(new_price_before_discount, new_price_after_discount) AS price
    FROM
        $temp_correct_comp_prices AS a
    INNER JOIN
        $temp_max_dt_comp AS m
    ON
        a.comp_date == m.dt_max AND a.competitor_name == m.competitor_name AND a.city == m.city AND a.item_monitored_id == m.item_monitored_id
);
$temp_comp_price_combined = (
    SELECT
        item_id,
        price_list,
        city,
        product_name,
        SerializeJson(From(AGGREGATE_LIST(AsTuple(competitor_name, price.0, price.1)))) AS prices,
        SerializeJson(From(AGGREGATE_LIST(AsTuple(competitor_name, price.0, price.1, source)))) AS prices_with_source
    FROM
        $temp_final_aggregated AS final
    LEFT JOIN
        $temp_comp_price_by_competitors AS comp
    USING (item_id, city)
    WHERE
        ABS(price.0 / retail_price_with_nds - 1) <= 0.5 OR ABS(price.1 / retail_price_with_nds - 1) <= 0.5
    GROUP BY
        final.item_id AS item_id,
        final.price_list AS price_list,
        final.city AS city,
        final.product_name AS product_name
);

$temp_private_label_flags = (
    SELECT
        CAST(item_id AS Int64) AS item_id
    FROM
        $master_category_data
    WHERE
        private_label_flag
);

$temp_federal_items = (
    SELECT
        item_id
    FROM
        $temp_final_aggregated
    WHERE
        city == 'City_Alpha'
);

$temp_pricing_rules_main = (
    SELECT
        final.item_id AS item_id,
        final.city AS city,
        final.price_list AS price_list,
        final.product_name AS product_name,
        final.purchase_price_without_nds AS purchase_price_without_nds,
        final.purchases_cost_with_nds AS purchase_price_with_nds,
        final.retail_price_with_nds AS retail_price_with_nds,
        final.supplier_id AS supplier_id,
        pnl.sales_quantity AS sales_quantity,
        pnl.front_margin_abs AS front_margin_abs,
        pnl.commercial_margin AS commercial_margin_abs,
        cat.category_name AS category_name,
        cat.subcategory_name AS subcategory_name,
        comp.prices AS comp_prices,
        Math::Round((final.retail_price_with_nds / (1 + final.output_tax) - final.purchase_price_without_nds) / (final.retail_price_with_nds / (1 + final.output_tax)), -2) AS front_margin
    FROM
        $temp_final_aggregated AS final
    LEFT JOIN
        $temp_pnl_by_city_data AS pnl
    ON
        final.item_id == pnl.item_id AND final.city == pnl.city
    LEFT JOIN
        $temp_product_categories AS cat
    ON
        CAST(final.item_id AS string) == cat.item_code
    LEFT JOIN
        $temp_comp_price_combined AS comp
    ON
        final.item_id == comp.item_id AND final.city == comp.city AND final.price_list == comp.price_list AND final.product_name == comp.product_name
    WHERE
        final.city IN $locations
);

$temp_active_items = (
    SELECT DISTINCT
        CAST(`item_id` AS int64) AS item_id,
        IF(`city_name_for_report_ru` == 'City_Gamma Franchise', 'City_Gamma', city_name_for_report_ru) AS city
    FROM
        `source/commercial_data/daily_report`
    WHERE
        CAST(date_dt AS string) == $day_before AND active_status IN ('Active', 'DC Stocks')
);

$temp_final_pricing_rules = (
    SELECT
        a.*
    FROM
        $temp_pricing_rules_main AS a
    INNER JOIN
        $temp_active_items AS b
    USING (item_id, city)
);

$temp_output_table = '//output/path/pricing_rules/output_data/' || $day_before;

INSERT INTO $temp_output_table WITH TRUNCATE
SELECT
    *
FROM
    $temp_final_pricing_rules
WHERE
    category_name NOT IN $excluded_categories AND price_list == 'Main'
;