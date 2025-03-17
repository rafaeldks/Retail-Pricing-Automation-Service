$previous_day = CAST(CurrentUtcDate() - Interval('P1D') AS String);
$current_day = CAST(CurrentUtcDate() AS String);
$source_path = '//output/path/pricing_rules/output_data/' || $previous_day;

$temp_table = (
    SELECT
        `comp_prices`,
        `category_name`,
        'test_city' AS `city`,
        `front_margin`,
        `item_id`,
        `outcoming_nds`,
        `product_name`,
        `purchase_price_with_nds`,
        `retail_price_with_nds`,
        `subcategory_name`
    FROM
        $source_path
);

$temp_table = (
    SELECT
        a.*,
        b.target_margin AS target_margin,
        b.line AS line
    FROM
        $temp_table AS a
    LEFT JOIN
        `//data/analytics/service/test_table_snapshot` AS b
    ON
        CAST(a.item_id AS string) == b.item_id
);

$temp_table = (
    SELECT
        a.*,
        b.new_sales AS new_sales,
        b.optimizer_price AS optimizer_price
    FROM
        $temp_table AS a
    JOIN
        `//data/analytics/service/optimizer_outputs` AS b
    ON
        CAST(a.item_id AS string) == b.item_id
);

$automator_path = '//data/analytics/pricing/automator_outputs/' || $current_day;

$temp_table = (
    SELECT
        a.*,
        b.new_price_final AS automator_price,
        b.reason AS reason
    FROM
        $temp_table AS a
    LEFT JOIN
        $automator_path AS b
    ON
        CAST(a.item_id AS string) == b.item_id AND a.city == b.city
);

INSERT INTO `//data/analytics/service/data_from_pr` WITH TRUNCATE
SELECT
    *
FROM
    $temp_table
ORDER BY
    item_id
;
