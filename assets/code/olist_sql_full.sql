/* =========================================================
Project Pack 2: Conversion Funnel + Attribution Optimization
SQL Part (MySQL)

Required source tables:
1) olist_marketing_qualified_leads_dataset
2) olist_closed_deals_dataset
3) olist_orders_dataset
4) olist_order_items_dataset
========================================================= */

/* 0) Data volume check */
SELECT 'mql' AS t, COUNT(*) AS n FROM olist_marketing_qualified_leads_dataset
UNION ALL
SELECT 'closed_deals', COUNT(*) FROM olist_closed_deals_dataset
UNION ALL
SELECT 'orders', COUNT(*) FROM olist_orders_dataset
UNION ALL
SELECT 'order_items', COUNT(*) FROM olist_order_items_dataset;

/* 1) Expand order items by seller with unified datetime */
DROP TEMPORARY TABLE IF EXISTS tmp_order_seller_gmv;
CREATE TEMPORARY TABLE tmp_order_seller_gmv AS
SELECT
    oi.order_id,
    oi.seller_id,
    (oi.price + oi.freight_value) AS line_gmv,
    CAST(o.order_purchase_timestamp AS DATETIME) AS order_purchase_ts
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o
  ON oi.order_id = o.order_id;

/* 2) 90-day post-win GMV per converted lead */
DROP TEMPORARY TABLE IF EXISTS tmp_mql_gmv90;
CREATE TEMPORARY TABLE tmp_mql_gmv90 AS
SELECT
    cd.mql_id,
    SUM(osg.line_gmv) AS gmv_90d
FROM olist_closed_deals_dataset cd
LEFT JOIN tmp_order_seller_gmv osg
  ON cd.seller_id = osg.seller_id
 AND osg.order_purchase_ts >= CAST(cd.won_date AS DATETIME)
 AND osg.order_purchase_ts <  DATE_ADD(CAST(cd.won_date AS DATETIME), INTERVAL 90 DAY)
GROUP BY cd.mql_id;

/* 3) Build top-12 campaign buckets from landing pages */
DROP TEMPORARY TABLE IF EXISTS tmp_top12_lp;
CREATE TEMPORARY TABLE tmp_top12_lp AS
SELECT landing_page_id
FROM olist_marketing_qualified_leads_dataset
GROUP BY landing_page_id
ORDER BY COUNT(*) DESC
LIMIT 12;

/* 4) Build master analysis table */
DROP TEMPORARY TABLE IF EXISTS tmp_master;
CREATE TEMPORARY TABLE tmp_master AS
SELECT
    mql.mql_id,
    CAST(mql.first_contact_date AS DATETIME) AS first_contact_date,
    CASE
      WHEN mql.origin IS NULL THEN 'unknown'
      WHEN TRIM(LOWER(mql.origin)) IN ('', 'nan', 'none', 'null') THEN 'unknown'
      ELSE TRIM(LOWER(mql.origin))
    END AS channel,
    CASE
      WHEN t.landing_page_id IS NOT NULL THEN LEFT(mql.landing_page_id, 8)
      ELSE 'other_lp'
    END AS campaign,
    CASE WHEN cd.mql_id IS NOT NULL THEN 1 ELSE 0 END AS converted,
    COALESCE(g.gmv_90d, 0) AS gmv_90d
FROM olist_marketing_qualified_leads_dataset mql
LEFT JOIN olist_closed_deals_dataset cd
  ON mql.mql_id = cd.mql_id
LEFT JOIN tmp_mql_gmv90 g
  ON mql.mql_id = g.mql_id
LEFT JOIN tmp_top12_lp t
  ON mql.landing_page_id = t.landing_page_id;

/* 5) Funnel summary: MQL -> Won -> Won with GMV in 90 days */
SELECT
    COUNT(*) AS mql_leads,
    SUM(converted) AS won_deals,
    ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS lead_to_won_cvr_pct,
    SUM(CASE WHEN gmv_90d > 0 THEN 1 ELSE 0 END) AS won_with_gmv,
    ROUND(100.0 * SUM(CASE WHEN gmv_90d > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS lead_to_pay_cvr_pct
FROM tmp_master;

/* 6) Channel efficiency (unit budget: one lead = one budget unit) */
SELECT
    channel,
    COUNT(*) AS leads,
    SUM(converted) AS wins,
    ROUND(SUM(gmv_90d), 2) AS gmv_90d,
    ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS cvr_pct,
    ROUND(SUM(gmv_90d) / COUNT(*), 2) AS roi_per_unit,
    ROUND(CASE WHEN SUM(converted) > 0 THEN 1.0 * COUNT(*) / SUM(converted) END, 2) AS cac_unit
FROM tmp_master
GROUP BY channel
HAVING COUNT(*) >= 50
ORDER BY roi_per_unit DESC;

/* 7) Campaign efficiency */
SELECT
    campaign,
    COUNT(*) AS leads,
    SUM(converted) AS wins,
    ROUND(SUM(gmv_90d), 2) AS gmv_90d,
    ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS cvr_pct,
    ROUND(SUM(gmv_90d) / COUNT(*), 2) AS roi_per_unit
FROM tmp_master
GROUP BY campaign
ORDER BY roi_per_unit DESC
LIMIT 20;

/* 8) Channel value distribution check */
SELECT channel, COUNT(*) AS leads
FROM tmp_master
GROUP BY channel
ORDER BY leads DESC;
