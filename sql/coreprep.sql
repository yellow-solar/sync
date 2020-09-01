-- PRODUCT AND PRICING GROUP TABLES 
INSERT INTO core.products (product, product_external_name)
	select distinct product, product from upya.stock
	where product is not null
		and product not in (select product from core.products)
		;

INSERT INTO core.products (product, product_external_name)
	select distinct product, product from upya.accounts
	where product is not null
		and product not in (select product from core.products)
		;

INSERT INTO core.pricing_groups(
	product, pricing_group)
	select distinct product, pricing_group from upya.accounts
	where pricing_group is not null
		and pricing_group not in (select pricing_group from core.pricing_groups)
;

INSERT INTO core.products (product, product_external_name)
	select distinct product, product from upya_uganda.stock
	where product is not null
		and product not in (select product from core.products)
		;

INSERT INTO core.products (product, product_external_name)
	select distinct product, product from upya_uganda.accounts
	where product is not null
		and product not in (select product from core.products)
		;

INSERT INTO core.pricing_groups(
	product, pricing_group)
	select distinct product, pricing_group from upya_uganda.accounts
	where pricing_group is not null
		and pricing_group not in (select pricing_group from core.pricing_groups)
;
