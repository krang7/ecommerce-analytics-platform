{{
    config(
        materialized='table',
        description='Product dimension with category and sales metrics'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

category_translation as (
    select * from {{ ref('stg_name_translation') }}
),

order_items as (
    select * from {{ ref('stg_items_ordered') }}
),

-- Aggregate product sales metrics
product_metrics as (
    select
        product_id,
        count(distinct order_id) as times_sold,
        sum(item_price) as total_revenue,
        avg(item_price) as avg_price,
        sum(shipping_price) as total_shipping_cost,
        avg(shipping_price) as avg_shipping_cost,
        min(item_id) as first_sold_order_item_id -- proxy for when first sold
    from order_items
    group by product_id
),

final as (
    select
        -- Product attributes
        p.product_id,
        p.product_category_name,
        ct.product_category_name_english as product_category,
        
        -- Product dimensions (if you have them in staging)
        
        -- Sales metrics
        coalesce(pm.times_sold, 0) as times_sold,
        coalesce(pm.total_revenue, 0) as total_revenue,
        pm.avg_price,
        pm.total_shipping_cost,
        pm.avg_shipping_cost,
        
        -- Calculated fields
        
        -- Product categorization
        case
            when pm.times_sold >= 100 then 'Best Seller'
            when pm.times_sold >= 50 then 'Popular'
            when pm.times_sold >= 10 then 'Regular'
            when pm.times_sold > 0 then 'Slow Moving'
            else 'Never Sold'
        end as sales_category,
        
        case
            when pm.avg_price >= 500 then 'Premium'
            when pm.avg_price >= 100 then 'Mid-Range'
            when pm.avg_price > 0 then 'Budget'
            else 'Unknown'
        end as price_category

    from products p
    left join category_translation ct 
        on p.product_category_name = ct.product_category_name
    left join product_metrics pm 
        on p.product_id = pm.product_id
)

select * from final