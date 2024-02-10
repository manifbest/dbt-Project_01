{#
with payments as (
select  * from {{ ref('stg_payments') }}
) , 

 pivoted as (
select  
ORDERID,
SUM(case when PAYMENTMETHOD='bank_transfer' then amount else 0 end ) as bank_transfer_amount,
SUM(case when PAYMENTMETHOD='credit_card' then amount else 0 end ) as credit_card_amount,
SUM(case when PAYMENTMETHOD='coupon' then amount else 0 end ) as coupon_amount,
SUM(case when PAYMENTMETHOD='gift_card' then amount else 0 end ) as gift_card_amount
 from payments
 where STATUS='success'
 Group by ORDERID
 )

 select  * from pivoted

 #}

 {% set payment_methods=['bank_transfer','credit_card','coupon','gift_card'] %}
with payments as (
select  * from {{ ref('stg_payments') }}
) , 
 pivoted as
 (
select  
orderId ,
{%- for payment in payment_methods -%}
SUM(case when PAYMENTMETHOD= '{{payment}}'  then amount else 0 end ) as {{payment}}_amount
{%-if not loop.last-%}
,
{%-endif%}
{%endfor-%}
from 
payments
where STATUS='success'
group by orderId
 )
 select  * from pivoted