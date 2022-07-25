-- Implement: https://www.codeproject.com/Questions/1161244/How-to-query-data-using-the-FIFO-method-for-invent
WITH cte AS (
  SELECT
    ROW_NUMBER() OVER(
      ORDER BY
        time_created, order_id
    ) AS srno,
    CASE
      WHEN side = 'sell' THEN 0 - volume_executed
      ELSE volume_executed
    END adjustment
  FROM
    portfolio
) SELECT
  *
  FROM cte;

-- Implement: https://stackoverflow.com/questions/43831286/postgres-fifo-query-calculate-profit-margin 
WITH cte_b AS (
  SELECT 
    ROW_NUMBER() OVER(
      ORDER BY
        time_created, order_id
    ) AS srno,
    order_id,
    price AS buy_price,
    generate_series AS buy_unit_qty
  FROM portfolio, generate_series(1, volume_executed)
  WHERE side = 'buy'
), cte_s AS (
  SELECT
    ROW_NUMBER() OVER(
      ORDER BY
        time_created, order_id
    ) AS srno,
    order_id,
    price AS sell_price,
    generate_series AS sell_unit_qty
  FROM portfolio, generate_series(1, volume_executed)
  WHERE side = 'sell'
) SELECT * 
  FROM cte_b FULL JOIN cte_s USING (srno); 
