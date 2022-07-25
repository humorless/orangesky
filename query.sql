SELECT
  ROW_NUMBER() OVER(
    ORDER BY
      time_created
  ) AS srno,
  order_id,
  price,
  side,
  cost,
  volume_executed
FROM
  portfolio;
