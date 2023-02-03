
Huynh Dat Hung K10

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

select 
         EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)) as month,
         Sum(totals.visits ) as Visit,
         Sum(totals.Pageviews) as Pageviews,
         Sum(totals.transactions) as Transactions,
         Sum(totals.transactionRevenue)/1000000 as transactionRevenues
         FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _TABLE_SUFFIX between '0101 'and '0331 '
group by month 

-- Query 02: Bounce rate per traffic source in July 2017
Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) order by total_visit DESC
#standardSQL

SELECT trafficSource.source as source ,
	 SUM(totals.visits ) as total_visits,
	 SUM(totals.bounces) as total_no_of_bounces,
	 Count(totals.bounces)/ Count(totals.visits ) as Bounce_rate
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by Source

-- Query 3: Revenue by traffic source by week, by month in June 2017


SELECT 
'MONTH' AS time_type,
EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)) as time,
trafficSource.source as source, 
Round(sum(totals.totalTransactionRevenue),2)/1000000 as revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY source,time
UNION ALL
SELECT 
'WEEK' AS time_type,
EXTRACT(ISOWEEK FROM PARSE_DATE("%Y%m%d", date)) as time,
trafficSource.source as source, 
Round(sum(totals.totalTransactionRevenue),2)/1000000 as revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _TABLE_SUFFIX between '0601 'and '0630 '
GROUP BY source,time



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL


SELECT
  'Thang 6' AS Time,
( SUM(total_pagesviews_purchase) / COUNT(users_pc) ) AS avg_pageviews_purchase,
( SUM(total_pagesviews_non_purchase) / COUNT(users_npc) ) AS avg_pageviews_non_purchase,
FROM (
SELECT
fullVisitorId AS users_pc,
SUM(totals.pageviews) AS total_pagesviews_purchase
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170630'
AND
totals.transactions >=1 
GROUP BY users_pc ),(
SELECT
fullVisitorId AS users_npc,
SUM(totals.pageviews) AS total_pagesviews_non_purchase
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170630'
AND
totals.transactions is null 
GROUP BY users_npc )
UNION ALL

SELECT
  'Thang 7' AS Time,
( SUM(total_pagesviews_purchase) / COUNT(users_pc) ) AS avg_pageviews_purchase,
( SUM(total_pagesviews_non_purchase) / COUNT(users_npc) ) AS avg_pageviews_non_purchase,
FROM (
SELECT
fullVisitorId AS users_pc,
SUM(totals.pageviews) AS total_pagesviews_purchase
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
AND
totals.transactions >=1 
GROUP BY users_pc ),(
SELECT
fullVisitorId AS users_npc,
SUM(totals.pageviews) AS total_pagesviews_non_purchase
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
AND
totals.transactions is null 
GROUP BY users_npc )




-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT
 'Thang 7' AS Month,
(SUM (total_transactions_per_user) / COUNT(fullVisitorId) ) AS avg_total_transactions_per_user
FROM (
SELECT
fullVisitorId,
SUM (totals.transactions) AS total_transactions_per_user
FROM
`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
AND totals.transactions IS NOT NULL
GROUP BY
fullVisitorId )

-- Query 06: Average amount of money spent per session
#standardSQL

SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
    sum(totals.totalTransactionRevenue)/count(totals.visits) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where 
    _table_suffix between '20170701' and '20170731'
    AND totals.transactions >= 1
GROUP BY month

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

WITH product as (
    SELECT
        fullVisitorId,
        product.v2ProductName,
        product.productRevenue,
        product.productQuantity 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(hits.product) as product
    Where 
        _table_suffix between '20170701' and '20170731'
        AND product.productRevenue IS NOT NULL
)

SELECT
    product.v2ProductName as other_purchased_products,
    SUM(product.productQuantity) as quantity
FROM product
WHERE 
    product.fullVisitorId IN (
        SELECT fullVisitorId
        FROM product
        WHERE product.v2ProductName LIKE "YouTube Men's Vintage Henley"

    )
    AND product.v2ProductName NOT LIKE "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity desc

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH product_view as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_product_view
    FROM `bigquery-public data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '2'
    GROUP BY month
)

, addtocart as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '3'
    GROUP BY month
)

, purchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '6'
    GROUP BY month
)

SELECT
    product_view.month,
    product_view.num_product_view,
    addtocart.num_addtocart,
    purchase.num_purchase,
    ROUND((num_addtocart/num_product_view)*100,2) as add_to_cart_rate,
    ROUND((num_purchase/num_product_view)*100,2) as purchase_rate
FROM product_view
JOIN addtocart USING(month)
JOIN purchase USING(month)
ORDER BY month