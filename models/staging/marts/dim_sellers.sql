{{
    config(
        materialized='table',
        description= 'Seller dimension with location and performance metrics'
    )
}}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select * from {{ ref('stg_items_ordered') }}
),

-- Aggregate seller metrics
seller_metrics as (
    select
        seller_id,
        count(distinct order_id) as total_orders,
        count(distinct product_id) as unique_products_sold,
        sum(item_price) as total_revenue,
        avg(item_price) as avg_order_value,
        sum(shipping_price) as total_shipping_cost
    from order_items
    group by seller_id
),

final as (
    select
        -- Seller attributes
        s.seller_id,
        s.sellers_zip_code ,
        s.seller_city as city,
        s.seller_state as state,
        
        -- Sales metrics
        coalesce(sm.total_orders, 0) as total_orders,
        coalesce(sm.unique_products_sold, 0) as unique_products_sold,
        coalesce(sm.total_revenue, 0) as total_revenue,
        sm.avg_order_value,
        sm.total_shipping_cost,
        
        -- Performance categories
        case
            when sm.total_orders >= 100 then 'Top Seller'
            when sm.total_orders >= 50 then 'High Volume'
            when sm.total_orders >= 10 then 'Active'
            when sm.total_orders > 0 then 'Low Volume'
            else 'Inactive'
        end as seller_tier,
        
        case
            when sm.avg_order_value >= 500 then 'Premium'
            when sm.avg_order_value >= 100 then 'Standard'
            when sm.avg_order_value > 0 then 'Budget'
            else 'No Sales'
        end as price_tier

    from sellers s
    left join seller_metrics sm on s.seller_id = sm.seller_id
)

select * from final