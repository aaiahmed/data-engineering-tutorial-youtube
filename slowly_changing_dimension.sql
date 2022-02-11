-- create dimension table
create table public.dim_employees (
    employee_skey bigint,
    lastname text,
    firstname text,
    extension text,
    email text,
    reportsto bigint,
    jobtitle text,
	office_city text,
    office_phone text,
    office_addressline1 text,
    office_addressline2 text,
    office_state text,
    office_country text,
    office_postalcode text,
    office_territory text,
	employeenumber bigint,
	officecode bigint,
	effective_date timestamp with time zone, 
	expiration_date timestamp with time zone,
	current_row boolean,
    hash text
);


-- create sequence
create sequence public.employees_skey_serial;

-- create stored procedure
-- call public.transform_dim_employees();
create or replace procedure public.transform_dim_employees()
language sql
as 
$$
	with current_dim as (
		select employeenumber,
			   hash
		from public.dim_employees
	), incoming_rows as (
		select  e.lastname,
			    e.firstname,
		        e.extension,
			    e.email,
				e.reportsto,
				e.jobtitle,
				o.city,
				o.phone,
				o.addressline1,
				o.addressline2,
				o.state,
				o.country,
				o.postalcode,
                o.territory,
				e.employeenumber,
				o.officecode,
				current_timestamp,
				NULL::timestamp with time zone,
				TRUE,
				md5(e::text || o::text) as hash
		from public.employees e
		inner join public.offices o
		using (officecode)
	), to_be_inserted as (
		select nextval('public.employees_skey_serial'),
			   i.*
		from incoming_rows i
		where (i.employeenumber, i.hash) not in (select employeenumber, hash
								                 from current_dim)
	), to_be_updated as (
		update public.dim_employees
		set expiration_date = current_timestamp,
			current_row = FALSE
		where (employeenumber, hash) not in (select employeenumber, hash
								             from incoming_rows)
		returning employeenumber
	)
	insert into public.dim_employees
	select * from to_be_inserted;
$$;