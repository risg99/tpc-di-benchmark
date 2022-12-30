-- dimcustomer
truncate table master.dimcustomer;
insert into master.dimcustomer
	with customer as (
		select
		  row_number() over(order by cm.c_id) as sk
		, cm.c_id
		, cm.c_tax_id
		, case
			when cm.actiontype = 'INACT' then 'INACTIVE'
			else 'ACTIVE'
		  end as status
		, cm.c_l_name
		, cm.c_f_name
		, cm.c_m_name
		, case
			when upper(cm.c_gndr) = 'M' or upper(cm.c_gndr) = 'F'
			then upper(cm.c_gndr)
			else 'U'
		  end as gender
		, cm.c_tier
		, cm.c_dob
		, cm.c_adline1
		, cm.c_adline2
		, cm.c_zipcode
		, cm.c_city
		, cm.c_state_prov
		, cm.c_ctry
		, case
			when cm.c_p_1_ctry_code is not null and cm.c_p_1_area_code is not null and cm.c_p_1_local is not null
			then '+' || cm.c_p_1_ctry_code || ' (' || cm.c_p_1_area_code || ') ' || cm.c_p_1_local || coalesce(cm.c_p_1_ext, '')

			when cm.c_p_1_ctry_code is null and cm.c_p_1_area_code is not null and cm.c_p_1_local is not null
			then '(' || cm.c_p_1_area_code || ') ' || cm.c_p_1_local || coalesce(cm.c_p_1_ext, '')

			when cm.c_p_1_area_code is null and cm.c_p_1_local is not null
			then cm.c_p_1_local || coalesce(cm.c_p_1_ext, '')

			else null
		  end as phone1
		, case
			when cm.c_p_2_ctry_code is not null and cm.c_p_2_area_code is not null and cm.c_p_2_local is not null
			then '+' || cm.c_p_2_ctry_code || ' (' || cm.c_p_2_area_code || ') ' || cm.c_p_2_local || coalesce(cm.c_p_2_ext, '')

			when cm.c_p_2_ctry_code is null and cm.c_p_2_area_code is not null and cm.c_p_2_local is not null
			then '(' || cm.c_p_2_area_code || ') ' || cm.c_p_2_local || coalesce(cm.c_p_2_ext, '')

			when cm.c_p_2_area_code is null and cm.c_p_2_local is not null
			then cm.c_p_2_local || coalesce(cm.c_p_2_ext, '')

			else null
		  end as phone2
		, case
			when cm.c_p_3_ctry_code is not null and cm.c_p_3_area_code is not null and cm.c_p_3_local is not null
			then '+' || cm.c_p_3_ctry_code || ' (' || cm.c_p_3_area_code || ') ' || cm.c_p_3_local || coalesce(cm.c_p_3_ext, '')

			when cm.c_p_3_ctry_code is null and cm.c_p_3_area_code is not null and cm.c_p_3_local is not null
			then '(' || cm.c_p_3_area_code || ') ' || cm.c_p_3_local || coalesce(cm.c_p_3_ext, '')

			when cm.c_p_3_area_code is null and cm.c_p_3_local is not null
			then cm.c_p_3_local || coalesce(cm.c_p_3_ext, '')

			else null
		  end as phone3
		, cm.c_prim_email
		, cm.c_alt_email
		, ntr.tx_name as nat_tx_name
		, ntr.tx_rate as nat_tx_rate
		, ltr.tx_name as lcl_tx_name
		, ltr.tx_rate as lcl_tx_rate
		, case
			when cm.actionts::date = max(cm.actionts::date) over(partition by cm.c_id range between unbounded preceding and unbounded following)
			then true
			else false
		  end as iscurrent
		, 1 as batchid
		, cm.actionts::date as effectivedate
		, '9999-12-31'::date as enddate
		, cm.actiontype
		from staging.customermgmt cm
		cross join staging.batchdate bd
		left join master.taxrate ntr
			on cm.c_nat_tx_id = ntr.tx_id
		left join master.taxrate ltr
			on cm.c_lcl_tx_id = ltr.tx_id
		where cm.actiontype in ('NEW', 'UPDCUST', 'INACT')
	)

	, c_new as (
		select
		*
		from customer
		where actiontype = 'NEW'
	)

	, c_not_new as (
		select
		  coalesce(c.sk, cn.sk) as sk
		, coalesce(c.c_id, cn.c_id) as c_id
		, coalesce(c.c_tax_id, cn.c_tax_id) as c_tax_id
		, coalesce(c.status, cn.status) as status
		, coalesce(c.c_l_name, cn.c_l_name) as c_l_name
		, coalesce(c.c_f_name, cn.c_f_name) as c_f_name
		, coalesce(c.c_m_name, cn.c_m_name) as c_m_name
		, coalesce(c.gender, cn.gender) as gender
		, coalesce(c.c_tier, cn.c_tier) as c_tier
		, coalesce(c.c_dob, cn.c_dob) as c_dob
		, coalesce(c.c_adline1, cn.c_adline1) as c_adline1
		, coalesce(c.c_adline2, cn.c_adline2) as c_adline2
		, coalesce(c.c_zipcode, cn.c_zipcode) as c_zipcode
		, coalesce(c.c_city, cn.c_city) as c_city
		, coalesce(c.c_state_prov, cn.c_state_prov) as c_state_prov
		, coalesce(c.c_ctry, cn.c_ctry) as c_ctry
		, coalesce(c.phone1, cn.phone1) as phone1
		, coalesce(c.phone2, cn.phone2) as phone2
		, coalesce(c.phone3, cn.phone3) as phone3
		, coalesce(c.c_prim_email, cn.c_prim_email) as c_prim_email
		, coalesce(c.c_alt_email, cn.c_alt_email) as c_alt_email
		, coalesce(c.nat_tx_name, cn.nat_tx_name) as nat_tx_name
		, coalesce(c.nat_tx_rate, cn.nat_tx_rate) as nat_tx_rate
		, coalesce(c.lcl_tx_name, cn.lcl_tx_name) as lcl_tx_name
		, coalesce(c.lcl_tx_rate, cn.lcl_tx_rate) as lcl_tx_rate
		, c.iscurrent
		, c.batchid
		, c.effectivedate
		, c.enddate
		, c.actiontype
		from customer c
		inner join c_new cn
			on c.c_id = cn.c_id
		where c.actiontype != 'NEW'
	)

	, c_all as (
		select * from c_new
		union all
		select * from c_not_new
	)

	, final_output as (
		select
		  cm.sk
		, cm.c_id
		, cm.c_tax_id
		, cm.status
		, cm.c_l_name
		, cm.c_f_name
		, cm.c_m_name
		, cm.gender
		, cm.c_tier
		, cm.c_dob
		, cm.c_adline1
		, cm.c_adline2
		, cm.c_zipcode
		, cm.c_city
		, cm.c_state_prov
		, cm.c_ctry
		, cm.phone1
		, cm.phone2
		, cm.phone3
		, cm.c_prim_email
		, cm.c_alt_email
		, cm.nat_tx_name
		, cm.nat_tx_rate
		, cm.lcl_tx_name
		, cm.lcl_tx_rate
		, case
			when cm.effectivedate = max(cm.effectivedate) over(partition by cm.c_id range between unbounded preceding and unbounded following)
			then p.agencyid
			else null
		  end as agencyid
		, case
			when cm.effectivedate = max(cm.effectivedate) over(partition by cm.c_id range between unbounded preceding and unbounded following)
			then p.creditrating
			else null
		  end as creditrating
		, case
			when cm.effectivedate = max(cm.effectivedate) over(partition by cm.c_id range between unbounded preceding and unbounded following)
			then p.networth
			else null
		  end as networth
		, case
			when cm.effectivedate = max(cm.effectivedate) over(partition by cm.c_id range between unbounded preceding and unbounded following)
			then p.marketingnameplate
			else null
		  end as marketingnameplate
		, cm.iscurrent
		, cm.batchid
		, cm.effectivedate
		, cm.enddate
		from c_all cm
		left join master.prospect p
			on upper(cm.c_l_name) = upper(p.lastname)
			and upper(cm.c_f_name) = upper(p.firstname)
			and upper(cm.c_adline1) = upper(p.addressline1)
			and upper(cm.c_adline2) = upper(p.addressline2)
			and upper(cm.c_zipcode) = upper(p.postalcode)
	)

	select * from final_output;