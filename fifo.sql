SELECT order_id, name, time_created, qty_sold
    -- 5
    --, 
    , case 
        when qty_sold = 0 then NULL
        else round((cum_sold_cost - coalesce(lag(cum_sold_cost) over w, 0))/qty_sold, 2)
      end fifo_price    
    , qty_bought, prev_bought, total_cost
    , prev_total_cost
    , cum_sold_cost
    , coalesce(lag(cum_sold_cost) over w, 0) as prev_cum_sold_cost
FROM (
    SELECT order_id, tneg.name, time_created, qty_sold, tpos.qty_bought, prev_bought, total_cost, prev_total_cost
        -- 4
        , round(prev_total_cost + ((tneg.cum_sold - tpos.prev_bought)/(tpos.qty_bought - tpos.prev_bought))*(total_cost-prev_total_cost), 2) as cum_sold_cost 
    FROM (
      SELECT order_id, name, time_created, volume_executed as qty_sold
          , sum(volume_executed) over w as cum_sold
      FROM portfolio
      WHERE side = 'sell'
      WINDOW w AS (PARTITION BY name ORDER BY time_created)
    -- 1
    ) tneg 
    LEFT JOIN (
      SELECT name
          , sum(volume_executed) over w as qty_bought
          , coalesce(sum(volume_executed) over prevw, 0) as prev_bought
          , volume_executed * price as cost                              
          , sum(volume_executed * price) over w as total_cost
          , coalesce(sum(volume_executed * price) over prevw, 0) as prev_total_cost
      FROM portfolio
      WHERE side = 'buy'
      WINDOW w AS (PARTITION BY name ORDER BY time_created)
          , prevw AS (PARTITION BY name ORDER BY time_created ROWS BETWEEN unbounded preceding AND 1 preceding)
    -- 2
    ) tpos 
    -- 3
    ON tneg.cum_sold BETWEEN tpos.prev_bought AND tpos.qty_bought 
        AND tneg.name = tpos.name
    ) t
WINDOW w AS (PARTITION BY name ORDER BY time_created)
ORDER BY time_created;
