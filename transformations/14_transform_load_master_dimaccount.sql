-- dimaccount
truncate table master.dimaccount;
insert into master.dimaccount
	with account as (
		select
		  row_number() over(order by cm.ca_id) as sk
		, cm.ca_id
		, b.sk_brokerid
		, c.sk_customerid
		, case
			when cm.actiontype in ('NEW', 'ADDACCT', 'UPDACCT', 'UPDCUST')
			then 'ACTIVE'
			else 'INACTIVE'
		  end as status
		, cm.ca_name
		, cm.ca_tax_st
		, case
			when cm.actionts::date = max(cm.actionts::date) over(partition by cm.ca_id range between unbounded preceding and unbounded following)
			then true
			else false
		  end as iscurrent
		, 1 as batchid
		, cm.actionts::date as effectivedate
		, '9999-12-31'::date as enddate
		, cm.actiontype
		from staging.customermgmt cm
		cross join staging.batchdate bd
		left join master.dimbroker b
			on cm.ca_b_id = b.brokerid
		left join master.dimcustomer c
			on cm.c_id = c.customerid
			and cm.actionts::date >= c.effectivedate
			and cm.actionts::date <= c.enddate
		where cm.actiontype in ('NEW', 'ADDACCT', 'UPDACCT', 'CLOSEACCT', 'UPDCUST', 'INACT')
	)
	
	, ca_new as (
			select
			*
			from account
			where actiontype = 'NEW'
	)
	
	, ca_not_new as (
		select
		  coalesce(a.sk, cn.sk) as sk
		, coalesce(a.ca_id, cn.ca_id) as ca_id
		, coalesce(a.sk_brokerid, cn.sk_brokerid) as sk_brokerid
		, coalesce(a.sk_customerid, cn.sk_customerid) as sk_customerid
		, coalesce(a.status, cn.status) as status
		, coalesce(a.ca_name, cn.ca_name) as ca_name
		, coalesce(a.ca_tax_st, cn.ca_tax_st) as ca_tax_st
		, coalesce(a.iscurrent, cn.iscurrent) as iscurrent
		, coalesce(a.batchid, cn.batchid) as batchid
		, coalesce(a.effectivedate, cn.effectivedate) as effectivedate
		, coalesce(a.enddate, cn.enddate) as enddate
		, a.actiontype
		from account a
		inner join ca_new cn
			on a.ca_id = cn.ca_id
		where a.actiontype != 'NEW'
	)
	
	, ca_all as (
		select * from ca_new
		union all
		select * from ca_not_new
	)

	select 
	  sk
	, ca_id
	, sk_brokerid
	, sk_customerid
	, status
	, ca_name
	, ca_tax_st
	, iscurrent
	, batchid
	, effectivedate
	, enddate
	from ca_all