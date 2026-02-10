with order_lines as (
    select * from {{ ref('stg_order_lines') }}
),

products as (
    select * from {{ ref('stg_products') }} -- On pointe sur le staging ici !
),

final as (
    select
        ol.order_line_id,
        ol.order_id,
        p.product_name,
        p.product_category,
        ol.quantity,
        ol.line_item_amount
    from order_lines ol
    left join products p on ol.product_id = p.product_id
)

select * from final