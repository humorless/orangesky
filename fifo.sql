with step1 as (
  select
    *,
    coalesce(
      sum(
        case
          when side = 'sell' then volume_executed
          else 0
        end
      ) over(
        order by
          time_created 
        rows between unbounded preceding
             and 1 preceding
      ),
      0
    ) previous_sold
  from
    portfolio
  order by
    time_created
),
step2_3 AS (
  SELECT
    *,
    (previous_running_stock - previous_sold) AS open_stock,
    (previous_running_stock - previous_sold - t1.volume_executed) AS close_stock,
    ROW_NUMBER() over(PARTITION BY t1.time_created order by (case when previous_running_stock - previous_sold - t1.volume_executed < 0  then null else 0 - t1.time_created end) desc) rnk
  FROM
    step1 AS t1,
    LATERAL (
      SELECT 
        time_created AS batch_order, cost AS batch_cost, volume_executed AS batch_qty,
        coalesce(
        sum(volume_executed) 
        over( 
          order by time_created 
          rows unbounded preceding
        ),
          0
        ) previous_running_stock
      FROM portfolio
      WHERE side = 'buy'
      AND time_created < t1.time_created  
    ) AS t2
  WHERE t1.side = 'sell' ) 
SELECT * 
FROM step2_3
 WHERE (open_stock > 0) AND
 (close_stock < 0 OR rnk = 1);
