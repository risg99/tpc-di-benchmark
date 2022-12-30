-- factholdings
truncate table master.factholdings;
insert into master.factholdings 
	select
	h.hh_h_t_id as tradeid,
	t.tradeid as currenttradeid,
	t.sk_customerid as sk_customerid,
	t.sk_accountid as sk_accountid,
	t.sk_securityid as sk_securityid,
	t.sk_companyid as sk_companyid,
	t.sk_closedateid as sk_dateid,
	t.sk_closetimeid as sk_timeid,
	t.tradeprice as currentprice,
	h.hh_after_qty as currentholding,
	1 as batchid
	from staging.holdinghistory h,
		master.dimtrade t
	where h.hh_t_id = t.tradeid;