with source as (
    select * from {{ ref('raw_order_lines') }}
),

renamed as (
    select
        order_line_id,
        order_id,
        product_id,
        quantity,
        price,
        (quantity * price) as line_item_amount
    from source
)

select * from renamed