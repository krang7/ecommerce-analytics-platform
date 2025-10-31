with source as (
    select * from {{ source('raw', 'olist_order_items_dataset') }}
),

renamed as (
    select
        order_id,
        order_item_id as item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_Date,
        price as item_price,
        freight_value as shipping_price 
    from source
)

select * from renamed