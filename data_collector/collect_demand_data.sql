$yesterday = CAST(CurrentUtcDate() - Interval('P1D') AS String);

$item_sales = (
    SELECT
        lcl_dt,
        item_id,
        some(`city_name`) AS city_name,
        sum(`gmv_w_vat_lcy`) / sum(`sold_item_cnt`) AS avg_price,
        sum(`osa_numerator_val`) / sum(`osa_denominator_val`) AS osa_perc,
        sum(`sold_item_cnt`) AS sales
    FROM
        range(`//data/source/com/metric_daily`, $start_date, $end_date)
    WHERE
        substring(lcl_dt, 0, 10) BETWEEN $start_date AND $end_date AND currency_code == 'RUB' AND city_name == 'test_city'
    GROUP BY
        item_id,
        substring(lcl_dt, 0, 10)
    HAVING
        (sum(`gmv_w_vat_lcy`) / sum(`sold_item_cnt`) IS NOT NULL) AND sum(`sold_item_cnt`) > 0 AND (sum(`osa_numerator_val`) / sum(`osa_denominator_val`)) IS NOT NULL
);

$item_sales_with_cats = (
    SELECT
        a.*,
        b.lvl3_category_name AS lvl3_category_name,
        b.lvl4_subcategory_name AS lvl4_subcategory_name,
        b.lvl5_subcategory_name AS lvl5_subcategory_name
    FROM
        $item_sales AS a
    INNER JOIN
        `//data/output/master_category/dim_records` AS b
    ON
        a.item_id == b.item_id
);

$path = '//output/path/pricing_rules/output_data/' || $yesterday;

$current_price = (
    SELECT
        item_id,
        retail_price_with_nds
    FROM
        $path
);

$final = (
    SELECT
        a.*,
        b.retail_price_with_nds
    FROM
        $item_sales_with_cats AS a
    INNER JOIN
        $current_price AS b
    ON
        a.item_id == b.item_id
);

INSERT INTO `//data/analytics/service/demand_prediction_data` WITH TRUNCATE
SELECT
    *
FROM
    $item_sales_with_cats
ORDER BY
    lcl_dt,
    item_id
;