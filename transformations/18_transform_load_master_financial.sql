-- financial
truncate table master.financial;
insert into master.financial 
	select 
		c.sk_companyid as sk_companyid,
		year::numeric as fi_year,
		quarter::numeric as fi_qtr,
		qtrstartdate::date as fi_qtr_start_date,
		revenue::numeric as fi_revenue,
		earnings::numeric as fi_net_earn,
		eps::numeric as fi_basic_eps,
		dilutedeps::numeric as fi_dilut_eps,
		margin::numeric as fi_margin,
		inventory::numeric as fi_inventory,
		assets::numeric as fi_assets,
		liability::numeric as fi_liability,
		shout::numeric as fi_out_basic,
		dilutedshout::numeric as fi_out_dilut
	from staging.finwire_fin f,
		master.dimcompany c
	where ((f.conameorcik = c.companyid::varchar) 
		or (f.conameorcik = c.name))
	and left(pts, 8)::date >= c.effectivedate
	and left(pts, 8)::date < c.enddate;