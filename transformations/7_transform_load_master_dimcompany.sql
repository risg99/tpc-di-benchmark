-- dimcompany
truncate table master.dimcompany;
insert into master.dimcompany
	select 
	row_number() over(order by cik) as sk,
	cik::numeric(11) as companyid, 
	s.st_name as status,
	companyname as name, 
	i.in_name as industry,
	(CASE 
		WHEN sprating not in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D') 
			THEN null
		ELSE f.sprating END) as sprating, 
	(CASE
		WHEN sprating not in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
			THEN null
		WHEN f.sprating like 'A%' or f.sprating like 'BBB%' 
			THEN false
		ELSE 
			true
	END) as islowgrade,
	ceoname as ceo,
	addressline1,
	addressline2,
	postalcode, 
	city, 
	stateprovince,
	country, 
	description, 
	foundingdate::date,
	case when lead( (select batchdate from staging.batchdate) ) over ( partition by cik order by pts asc ) is null then true else false end as iscurrent,
	1 as batchid,
	left(f.pts, 8)::date as effectivedate,
	'9999-12-31'::date as enddate 
	from 
		staging.finwire_cmp f, 
		staging.statustype s,
		staging.industry i
	where 
		f.status = s.st_id 
	and f.industryid = i.in_id;