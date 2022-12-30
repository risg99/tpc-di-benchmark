-- update prospect
with current_active_customer as (
	select p.*
	from master.prospect p
	inner join master.dimcustomer c
	on upper(c.lastname) = upper(p.lastname)
	and upper(c.firstname) = upper(p.firstname)
	and upper(c.addressline1) = upper(p.addressline1)
	and upper(c.addressline2) = upper(p.addressline2)
	and upper(c.postalcode) = upper(p.postalcode)
	where c.status = 'ACTIVE'
	and c.iscurrent = true
)

update master.prospect
	set iscustomer = true 
	where lastname in (select lastname from current_active_customer)
	and firstname in (select firstname from current_active_customer)
	and addressline1 in (select addressline1 from current_active_customer)
	and addressline2 in (select addressline2 from current_active_customer)
	and postalcode in (select postalcode from current_active_customer);