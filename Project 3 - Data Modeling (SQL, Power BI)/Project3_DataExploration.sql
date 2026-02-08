/*
===============================================================================
Business Analysis & KPI Calculation
Database: Olist Store
Author: Yaroslav_Pryimak
===============================================================================
*/

-- ----------------------------------------------------------------------------
-- 1. Top 3 Best-Selling Sellers per State
-- Description: Identifies the top 3 sellers by revenue within each state 
-- using the DENSE_RANK() window function to handle ties appropriately.
-- ----------------------------------------------------------------------------
WITH seller_rankings AS (
    SELECT 
        d.seller_state,
        d.seller_id,
        SUM(f.price) AS total_revenue,
        DENSE_RANK() OVER (PARTITION BY d.seller_state ORDER BY SUM(f.price) DESC) AS rank_in_state
    FROM fact_order_items f
    JOIN dim_sellers d 
        ON f.seller_id = d.seller_id
    GROUP BY d.seller_state, d.seller_id
)
SELECT 
    seller_state,
    seller_id,
    total_revenue,
    rank_in_state
FROM seller_rankings
WHERE rank_in_state <= 3
ORDER BY seller_state, total_revenue DESC;


-- ----------------------------------------------------------------------------
-- 2. Monthly Revenue Running Total
-- Description: Calculates the cumulative revenue growth month over month.
-- Insight: Shows how the total business volume accumulated over time.
-- ----------------------------------------------------------------------------
WITH monthly_sales AS (
    SELECT 
        DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS sale_month,
        SUM(payment_value) AS monthly_revenue
    FROM fact_orders o
    JOIN fact_order_payments p 
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('unavailable', 'canceled')
    GROUP BY 1
)
SELECT 
    sale_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (ORDER BY sale_month) AS running_total_revenue
FROM monthly_sales
ORDER BY sale_month;


-- ----------------------------------------------------------------------------
-- 3. RFM Segmentation (Recency, Frequency, Monetary)
-- Description: Segments customers based on purchasing behavior.
-- Logic:
--   - VIP: High Spenders (> 500) OR Frequent Buyers (> 3 orders)
--   - Loyal: Purchased recently (within last 90 days)
--   - At Risk: Inactive for 3-9 months
--   - Churned: Inactive for > 9 months (270 days)
-- Note: '2018-09-01' is used as the "current date" snapshot for this dataset.
-- ----------------------------------------------------------------------------
WITH customer_rfm AS (
    SELECT 
        d.customer_unique_id,
        MAX(o.order_purchase_timestamp) AS last_order_date,
        COUNT(DISTINCT o.order_id) AS frequency, 
        SUM(p.payment_value) AS monetary
    FROM fact_orders o
    JOIN fact_order_payments p ON o.order_id = p.order_id
    JOIN dim_customers d ON o.customer_id = d.customer_id 
    WHERE o.order_status = 'delivered'
    GROUP BY d.customer_unique_id
),
rfm_calc AS (
    SELECT *,
        DATEDIFF('2018-09-01', last_order_date) AS recency_days
    FROM customer_rfm
)
SELECT 
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    CASE 
        WHEN monetary > 500 OR frequency >= 3 THEN 'VIP'
        WHEN recency_days <= 90 THEN 'Loyal/Active'
        WHEN recency_days BETWEEN 91 AND 270 THEN 'At Risk'
        ELSE 'Churned'
    END AS customer_segment
FROM rfm_calc;


-- ----------------------------------------------------------------------------
-- 4. Logistics Route Analysis
-- Description: Identifies the most problematic routes based on delay rates.
-- Metrics:
--   - Delay Rate: Percentage of orders delivered after the estimated date.
--   - Avg Delay: Average days of delay (for late orders only).
-- ----------------------------------------------------------------------------
SELECT 
    CONCAT(s.seller_state, ' -> ', c.customer_state) AS route,
    COUNT(*) AS total_orders,
    
    -- Calculate % of orders that were late
    ROUND(AVG(CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 
        ELSE 0 
    END) * 100, 2) AS delay_rate_pct,
    
    -- Calculate average delay only for late orders
    ROUND(AVG(CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date 
        THEN DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)
        ELSE NULL 
    END), 1) AS avg_delay_days

FROM fact_orders o
JOIN fact_order_items oi ON o.order_id = oi.order_id
JOIN dim_sellers s ON oi.seller_id = s.seller_id
JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY route
HAVING total_orders > 10 -- Filter out rare routes to focus on statistically significant ones
ORDER BY delay_rate_pct DESC
LIMIT 10;