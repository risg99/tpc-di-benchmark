drop schema if exists master cascade;
create schema master authorization postgres;

drop table if exists master.tradetype;
create table master.tradetype(
	tt_id char(3) not null,
	tt_name char(12) not null, 
	tt_is_sell numeric(1) not null check(tt_is_sell >= 0),
	tt_is_mrkt numeric(1) not null check(tt_is_mrkt >= 0)
);

drop table if exists master.statustype;
create table master.statustype(
	st_id char(4) not null,
	st_name char(10) not null
);

drop table if exists master.taxrate;
create table master.taxrate(
	tx_id char(4) not null,
	tx_name char(50) not null,
	tx_rate numeric(6,5) not null check(tx_rate >= 0)
);

drop table if exists master.industry;
create table master.industry(
	in_id char(2) not null,
	in_name char(50) not null,
	in_sc_id char(4) not null
);

drop table if exists master.dimdate;
create table master.dimdate(
	sk_dateid numeric(11) not null check(sk_dateid >= 0),
	datevalue date not null,
	datedesc char(20) not null,
	calendaryearid numeric(4) not null check(calendaryearid >= 0),
	calendaryeardesc char(20) not null,
	calendarqtrid numeric(5) not null check(calendarqtrid >= 0),
	calendarqtrdesc char(20) not null,
	calendarmonthid numeric(6) not null check(calendarmonthid >= 0),
	calendarmonthdesc char(20) not null,	
	calendarweekid numeric(6) not null check(calendarweekid >= 0),
	calendarweekdesc char(20) not null,	
	dayofweeknum numeric(1) not null check(dayofweeknum >= 0),
	dayofweekdesc char(10) not null,	
	fiscalyearid numeric(4) not null check(fiscalyearid >= 0),
	fiscalyeardesc char(20) not null,	
	fiscalqtrid numeric(5) not null check(fiscalqtrid >= 0),
	fiscalqtrdesc char(20) not null,	
	holidayflag boolean
);

drop table if exists master.dimtime;
create table master.dimtime(
	sk_timeid numeric(11) not null check(sk_timeid >= 0),
	timevalue time not null,
	hourid numeric(2) not null check(hourid >= 0),
	hourdesc char(20) not null,
	minuteid numeric(2) not null check(minuteid >= 0),
	minutedesc char(20) not null,
	secondid numeric(2) not null check(secondid >= 0),
	seconddesc char(20) not null,
	markethoursflag boolean,
	officehoursflag boolean
);

drop table if exists master.dimcompany;
create table master.dimcompany(
	sk_companyid numeric(11) not null check(sk_companyid >= 0),
	companyid numeric(11) not null check(companyid >= 0),
	status char(10) not null,
	name char(60) not null,
	industry char(50) not null,
	sprating char(4),
	islowgrade boolean,
	ceo char(100) not null,
	addressline1 char(80),
	addressline2 char(80),
	postalcode char(12) not null,
	city char(25) not null,
	stateprov char(20) not null,
	country char(24),
	description char(150) not null,
	foundingdate date,
	iscurrent boolean not null,
	batchid numeric(5) not null check(batchid >= 0),
	effectivedate date not null,
	enddate date not null
);

drop table if exists master.dimbroker;
create table master.dimbroker(
	sk_brokerid numeric(11) not null check(sk_brokerid >= 0),
	brokerid numeric(11) not null check(brokerid >= 0),
	managerid numeric(11) check(managerid >= 0),
	firstname char(50) not null,
	lastname char(50) not null,
	middleinitial char(1),
	branch char(50),
	office char(50),
	phone char(14),
	iscurrent boolean not null,
	batchid numeric(5) not null check(batchid >= 0),
	effectivedate date not null,
	enddate date not null
);

drop table if exists master.dimcustomer;
create table master.dimcustomer(
	sk_customerid numeric(11) not null check(sk_customerid >= 0),
	customerid numeric(11) not null check(customerid >= 0),
	taxid char(20) not null,
	status char(10) not null,
	lastname char(30) not null,
	firstname char(20) not null,
	middleinitial char(1),
	gender char(1),
	tier numeric(1) check(tier >= 0),
	dob date not null,
	addressline1 char(80) not null,
	addressline2 char(80),
	postalcode char(12) not null,
	city char(25) not null,
	stateprov char(20) not null,
	country char(24),
	phone1 char(30),
	phone2 char(30),
	phone3 char(30),
	email1 char(50),
	email2 char(50),
	nationaltaxratedesc char(50),
	nationaltaxrate numeric(6,5) check(nationaltaxrate >= 0),
	localtaxratedesc char(50),
	localtaxrate numeric(6,5) check(localtaxrate >= 0),
	agencyid char(30),
	creditrating numeric(5) check(creditrating >= 0),
	networth numeric(10),
	marketingnameplate char(100),
	iscurrent boolean not null,
	batchid numeric(5) not null check(batchid >= 0),
	effectivedate date not null,
	enddate date not null
);

drop table if exists master.dimaccount;
create table master.dimaccount(
	sk_accountid numeric(11) not null check(sk_accountid >= 0),
	accountid numeric(11) not null check(accountid >= 0),
	sk_brokerid numeric(11) not null check(sk_brokerid >= 0),
	sk_customerid numeric(11) not null check(sk_customerid >= 0),
	status char(10) not null,
	accountdesc char(50),
	taxstatus numeric(1) check(taxstatus in(0, 1, 2)),
	iscurrent boolean not null,
	batchid numeric(5) not null check(batchid >= 0),
	effectivedate date not null,
	enddate date not null
);

drop table if exists master.dimsecurity;
create table master.dimsecurity(
	sk_securityid numeric(11) not null check(sk_securityid >= 0),
	symbol char(15) not null,
	issue char(6) not null,
	status char(10) not null,
	name char(70) not null,
	exchangeid char(6) not null,
	sk_companyid numeric(11) not null check(sk_companyid >= 0),
	sharesoutstanding numeric(12) not null check(sharesoutstanding >= 0),
	firsttrade date not null,
	firsttradeonexchange date not null,
	dividend numeric(10,2) not null,
	iscurrent boolean not null,
	batchid numeric(5) not null check(batchid >= 0),
	effectivedate date not null,
	enddate date not null 
);

drop table if exists master.dimtrade;
create table master.dimtrade(
	tradeid numeric(11) not null check(tradeid >= 0),
	sk_brokerid numeric(11) check(sk_brokerid >= 0),
	sk_createdateid numeric(11) not null check(sk_createdateid >= 0),
	sk_createtimeid numeric(11) not null check(sk_createtimeid >= 0),
	sk_closedateid numeric(11) check(sk_closedateid >= 0),
	sk_closetimeid numeric(11) check(sk_closetimeid >= 0),
	status char(10) not null,
	type char(12) not null,
	cashflag boolean not null,
	sk_securityid numeric(11) not null check(sk_securityid >= 0),
	sk_companyid numeric(11) not null check(sk_companyid >= 0),
	quantity numeric(6, 0) not null check(quantity >= 0),
	bidprice numeric(8, 2) not null check(bidprice >= 0),
	sk_customerid numeric(11) not null check(sk_customerid >= 0),
	sk_accountid numeric(11) not null check(sk_accountid >= 0),
	executedby char(64) not null,
	tradeprice numeric(8,2) check(tradeprice >= 0),
	fee numeric(10,2) check(fee >= 0),
	commission numeric(10,2) check(commission >= 0),
	tax numeric(10,2) check(tax >= 0),
	batchid numeric(5) not null check(batchid >= 0)
);

drop table if exists master.financial;
create table master.financial(
	sk_companyid numeric(11) not null check(sk_companyid >= 0),
	fi_year numeric(4) not null check(fi_year >= 0),
	fi_qtr numeric(1) not null check(fi_qtr >= 0),
	fi_qtr_start_date date not null,
	fi_revenue numeric(15, 2) not null,
	fi_net_earn numeric(15, 2) not null,
	fi_basic_eps numeric(10, 2) not null,
	fi_dilut_eps numeric(10, 2) not null,
	fi_margin numeric(10, 2) not null,
	fi_inventory numeric(15, 2) not null,
	fi_assets numeric(15, 2) not null,
	fi_liability numeric(15, 2) not null,
	fi_out_basic numeric(12) not null,
	fi_out_dilut numeric(12) not null
);

drop table if exists master.factcashbalances;
create table master.factcashbalances(
	sk_customerid numeric(11) not null check(sk_customerid >= 0),
	sk_accountid numeric(11) not null check(sk_accountid >= 0),
	sk_dateid numeric(11) not null check(sk_dateid >= 0),
	cash numeric(15, 2) not null,
	batchid numeric(5) not null check(batchid >= 0)
);

drop table if exists master.factholdings;
create table master.factholdings(
	tradeid numeric(11) not null check(tradeid >= 0),
	currenttradeid numeric(11) not null check(currenttradeid >= 0),
	sk_customerid numeric(11) not null check(sk_customerid >= 0),
	sk_accountid numeric(11) not null check(sk_accountid >= 0),
	sk_securityid numeric(11) not null check(sk_securityid >= 0),
	sk_companyid numeric(11) not null check(sk_companyid >= 0),
	sk_dateid numeric(11) not null check(sk_dateid >= 0),
	sk_timeid numeric(11) not null check(sk_timeid >= 0),
	currentprice numeric(8, 2) not null check(currentprice >= 0),
	currentholding numeric(6) not null,
	batchid numeric(5) not null check(batchid >= 0)
);

drop table if exists master.factmarkethistory;
create table master.factmarkethistory(
	sk_securityid numeric(11) not null check(sk_securityid >= 0),
	sk_companyid numeric(11) not null check(sk_companyid >= 0),
	sk_dateid numeric(11) not null check(sk_dateid >= 0),
	peratio numeric(10, 2) check(peratio >= 0),
	yield numeric(5, 2) not null check(yield >= 0),
	fiftytwoweekhigh numeric(8, 2) not null check(fiftytwoweekhigh >= 0),
	sk_fiftytwoweekhighdate numeric(11) not null check(sk_fiftytwoweekhighdate >= 0),
	fiftytwoweeklow numeric(8, 2) not null check(fiftytwoweeklow >= 0),
	sk_fiftytwoweeklowdate numeric(11) not null check(sk_fiftytwoweeklowdate >= 0),
	closeprice numeric(8, 2) not null check(closeprice >= 0),
	dayhigh numeric(8, 2) not null check(dayhigh >= 0),
	daylow numeric(8, 2) not null check(daylow >= 0),
	volume numeric(12) not null check(volume >= 0),
	batchid numeric(5) not null check(batchid >= 0)
);

drop table if exists master.factwatches;
create table master.factwatches(
	sk_customerid numeric(11) not null check(sk_customerid >= 0),
	sk_securityid numeric(11) not null check(sk_securityid >= 0),
	sk_dateid_dateplaced numeric(11) not null check(sk_dateid_dateplaced >= 0),
	sk_dateid_dateremoved numeric(11) check(sk_dateid_dateremoved >= 0),
	batchid numeric(5) not null check(batchid >= 0)
);

drop table if exists master.prospect;
create table master.prospect(
	agencyid char(30) not null,
	sk_recorddateid numeric(11) not null check(sk_recorddateid >= 0),
	sk_updatedateid numeric(11) not null check(sk_updatedateid >= 0),
	batchid numeric(5) not null check(batchid >= 0),
	iscustomer boolean not null,
	lastname char(30) not null,
	firstname char(30) not null,
	middleinitial char(1),
	gender char(1),
	addressline1 char(80),
	addressline2 char(80),
	postalcode char(12),
	city char(25) not null,
	state char(20) not null,
	country char(24),
	phone char(30),
	income numeric(9) check(income >= 0),
	numbercars numeric(2) check(numbercars >= 0),
	numberchildren numeric(2) check(numbercars >= 0),
	maritalstatus char(1),
	age numeric(3) check(age >= 0),
	creditrating numeric(4) check(creditrating >= 0),
	ownorrentflag char(1),
	employer char(30),
	numbercreditcards numeric(2) check(numbercreditcards >= 0),
	networth numeric(12) check(networth >= 0),
	marketingnameplate char(100)
);

-- operational tables
drop table if exists master.audit;
create table master.audit(
	dataset char(20) not null,
	batchid numeric(5) check(batchid >= 0),
	date date,
	attribute char(50) not null,
	value numeric(15),
	dvalue numeric(15, 5)
);

drop table if exists master.dimessages;
create table master.dimessages(
	messagedateandtime timestamp not null,
	batchid numeric(5) not null check(batchid >= 0),
	messagesource char(30),
	messagetext char(50) not null,
	messagetype char(12) not null,
	messagedata char(100)
);