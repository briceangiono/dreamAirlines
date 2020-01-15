-- Set Database context to DreamAirlines_DW
use DreamAirlines_DW; 
GO

-- 1
SELECT * FROM [DreamAirlines_DW].[dw].[DimHotelBooking];
SELECT count(*) as Cnt_Rows FROM [DreamAirlines_DW].[dw].[DimHotelBooking];

-- 2 
SELECT distinct DSC_Employee FROM [DreamAirlines_DW].[dw].[DimHotelBooking];

-- 3
SELECT DSC_Customer FROM [DreamAirlines_DW].[dw].[DimCustomer] WHERE DSC_Customer LIKE 'A%';

-- 4
SELECT distinct DSC_TravelDestiny FROM [DreamAirlines_DW].[dw].[DimTravelBooking] WHERE DSC_TravelDestiny NOT LIKE '%A';

-- 5
SELECT distinct *, substring(upper(DSC_TravelDestiny), 1, 2) as COD_TravelDestiny FROM [DreamAirlines_DW].[dw].[DimHotelBooking];

--or
SELECT distinct *, left(upper(DSC_TravelDestiny), 2) as COD_TravelDestiny FROM [DreamAirlines_DW].[dw].[DimHotelBooking];

-- 6
SELECT * FROM [DreamAirlines_DW].[dw].[DimHotelBooking] WHERE DSC_TravelDestiny LIKE 'Brazil';

-- 7
SELECT  distinct DSC_Employee, DSC_TravelAgency FROM [DreamAirlines_DW].[dw].[DimHotelBooking] ORDER BY DSC_TravelAgency;

-- 8
SELECT DSC_Employee,  DATEDIFF(YEAR, cast(cast(Contract_Date as varchar) as datetime), getdate()) as Antiquity_Compagny 
FROM [DreamAirlines_DW].[dw].[DimEmployee] 
ORDER BY Antiquity_Compagny DESC;

-- 9
--select * from [DreamAirlines_DW].[dw].[DimEmployee];
--select charindex(' ', DSC_Employee) from [DreamAirlines_DW].[dw].[DimEmployee];
--select a.PK_Employee, a.FK_Travel_Agency, left(a.DSC_Employee, charindex(' ', a.DSC_Employee) - 1), a.Birthdate, a.DSC_JobTitle, a.DSC_Address from [DreamAirlines_DW].[dw].[DimEmployee] a;
select 
	left(a.DSC_Employee, charindex(' ', a.DSC_Employee) - 1) as firstname, 
	substring(a.DSC_Employee, charindex(' ', a.DSC_Employee), len(a.DSC_Employee)) as lastname, 
	a.* 
from [DreamAirlines_DW].[dw].[DimEmployee] a;

-- 10
--select * from [dw].[DimHotel];

select 
	concat(h.DSC_Hotel, ' (', h.DSC_HotelCountry, ')') as DSC_HotelCountry, 
	h.* 
from [dw].[DimHotel] h;

-- 11
select cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime) as Birthdate , c.* from [dw].[DimCustomer] c;

-- 12
select datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) as Age, c.* 
from [dw].[DimCustomer] c
order by Age;

-- 13
select a.SegmentAge, count(*) as Amt_Customers from 
	(select 
			case 
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) between 10 and 17 then 'Segment A'  
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) between 18 and 24 then 'Segment B'
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) between 25 and 34 then 'Segment C' 
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) between 35 and 44 then 'Segment D'
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) between 45 and 54 then 'Segment E'
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) between 55 and 64 then 'Segment F'
				when datediff(year, cast(cast(cast(c.Birthdate as varchar) as varchar) as datetime), GETDATE()) > 64 then 'Segment G'
			end as SegmentAge
			from [dw].[DimCustomer] c 
	) as a
group by a.SegmentAge 
order by Amt_Customers desc;

-- 14
select top 1 e.DSC_Employee, datediff(year, cast(cast(e.Birthdate as varchar) as datetime), GETDATE()) as Age
from [dw].[DimEmployee] e
order by Age desc;

-- 15
select r.PK_CarRenting, c.DSC_Car 
from [dw].[DimCar] c left outer join [dw].[DimCarRenting] r
on c.PK_Car = r.FK_Car
where r.PK_CarRenting is NULL;

-- 16
select max([Amt_Salary]) as Highest_Salary from [dw].[DimEmployee];

-- 17
select min([Amt_Salary]) as Lowest_Salary from [dw].[DimEmployee];

-- 18
select top 1 b.DSC_Hotel, b.DSC_HotelService, f.AMT_Accomodation
from [dw].[FactHotelBooking] f
join [dw].[DimHotelBooking] b
on f.PK_HotelBooking = b.PK_HotelBooking
where b.DSC_HotelService like 'Breakfast'
order by f.AMT_Accomodation;

-- 19
select distinct c.DSC_RentalCompany, c.DSC_Car, max(f.AMT_DailyRate) over (partition by c.DSC_RentalCompany) as AMT_DailyRate from [dw].[DimCar] c
join [dw].[FactCarRenting] f
on c.PK_Car = f.FK_Car;

-- 20
select top 1 c.DSC_Car, r.AMT_Rental from [dw].[DimCar] c
join [dw].[FactCarRenting] r
on c.PK_Car = r.FK_Car
join [dw].[DimDate] d
on r.FK_Date = d.PK_Date
where d.MMYYYY = '072017'
order by r.AMT_Rental desc;

-- 21
select c.DSC_Car, sum(r.AMT_CarInsurance) AS AMT_Insurance from [dw].[DimCar] c
join [dw].[FactCarRenting] r
on c.PK_Car = r.FK_Car
join [dw].[DimDate] d
on r.FK_Date = d.PK_Date
where d.Year = '2016' and AMT_Insurance < 8000
order by r.AMT_CarInsurance desc, c.DSC_Car asc;

-- If we don't use the DimDate table  
select c.DSC_Car, sum(r.AMT_CarInsurance) AS AMT_Insurance from [dw].[DimCar] c
join [dw].[FactCarRenting] r
on c.PK_Car = r.FK_Car
where r.FK_Date like '2016%' and AMT_Insurance < 8000
group by c.DSC_Car
order by r.AMT_CarInsurance desc, c.DSC_Car asc;

-- 22
select distinct top 5 b.DSC_Employee, count(b.PK_TravelBooking) as NbTravelBookings 
from [dw].[DimTravelBooking] b
group by b.DSC_Employee
order by NbTravelBookings desc;

select distinct top 5 b.DSC_Employee, count(b.PK_TravelBooking) over (partition by b.dsc_employee) as NbTravelBookings 
from [dw].[DimTravelBooking] b
order by NbTravelBookings desc;

-- 23
with TopEmployee as (
	select 
		b.DSC_TravelAgency, 
		b.DSC_Employee, 
		count(b.PK_TravelBooking) as NbTravelBookings, 
		ROW_NUMBER() over (partition by DSC_TravelAgency order by count(b.PK_TravelBooking) desc) as rankRow
	from [dw].[DimTravelBooking] b
	group by b.DSC_TravelAgency, b.DSC_Employee
)
select DSC_TravelAgency, DSC_Employee, NbTravelBookings 
from TopEmployee where rankRow <= 3
order by DSC_TravelAgency, NbTravelBookings desc;

-- 24
with TopEmployee as (
	select 
		b.DSC_TravelAgency, 
		b.DSC_Employee, 
		count(b.PK_TravelBooking) as NbTravelBookings, 
		ROW_NUMBER() over (order by count(b.PK_TravelBooking) desc) as TopRank,
		ROW_NUMBER() over (order by count(b.PK_TravelBooking) asc) as BottomRank
	from [dw].[DimTravelBooking] b
	group by b.DSC_TravelAgency, b.DSC_Employee
)
select DSC_TravelAgency, DSC_Employee, NbTravelBookings 
from TopEmployee where TopRank <= 3 or BottomRank <= 3
order by NbTravelBookings desc;

-- 25
-- What is in the Screenshot
select e.DSC_Employee, round(sum(i.AMT_Travel_Insurance), 2) as Amt_Insurance from [dw].[DimEmployee] e
join [dw].[FactInsurance] i
on e.PK_Employee = i.FK_Employee
join [dw].[DimInsurance] d
on i.PK_Insurance = d.PK_Insurance
where i.AMT_Travel_Insurance is not null
group by e.DSC_Employee
order by sum(i.AMT_Travel_Insurance) desc;

-- The results
select e.DSC_Employee, round(sum(i.AMT_Travel_Insurance), 2) as Amt_Insurance from [dw].[DimEmployee] e
join [dw].[FactInsurance] i
on e.PK_Employee = i.FK_Employee
join [dw].[DimInsurance] d
on i.PK_Insurance = d.PK_Insurance
where d.DSC_Insurance not like 'Standard%' and i.AMT_Travel_Insurance is not null
group by e.DSC_Employee
order by sum(i.AMT_Travel_Insurance) desc;

-- 26
select e.DSC_Employee, e.DSC_JobTitle, e.Amt_Salary, datediff(year, cast(cast(e.Contract_Date as varchar) as datetime), GETDATE()) as Seniority 
from [dw].[DimEmployee] e
order by Seniority desc;

-- 27
select d.Year, d.Month, sum(b.AMT_Travel) as Amt_Travel
from [dw].[FactTravelBooking] b join [dw].[DimDate] d
on b.FK_date = d.PK_Date
group by d.Year, d.Month
order by d.Year, d.Month;

-- 28
select distinct e.DSC_Employee, d.Year, cast(avg(b.AMT_Travel) over(partition by e.DSC_Employee) as numeric(10,2)) as AVG_Amt_Travel
from [dw].[FactTravelBooking] b 
join [dw].[DimEmployee] e
on b.FK_Employee = e.PK_Employee
join [dw].[DimDate] d
on b.FK_date = d.PK_Date
where d.Year = 2016
order by AVG_Amt_Travel desc;

-- 29
select distinct e.DSC_Employee, (sum(b.AMT_Travel) over (partition by e.DSC_Employee) / sum(b.AMT_Travel) over ()) * 100 as PercentTotal
from [dw].[FactTravelBooking] b 
join [dw].[DimEmployee] e
on b.FK_Employee = e.PK_Employee
where datediff(year, cast(cast(e.Contract_Date as varchar) as datetime), GETDATE()) >= 5
order by PercentTotal desc;

-- 30
begin tran;

alter table [DreamAirlines_DW].[dw].[DimCar] add DSC_CarColor nvarchar(25) NULL;

-- 31
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Nissan Micra  1.0';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Volkswagen UP 1.0';
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Peugeot 108 1.0';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Smart Forfour 1.1';
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Renault Twingo 1.2';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Volkswagen Polo 1.2';
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Opel Adam 1.4 D';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Opel Corsa 1.4 D';
update [dw].[DimCar] set DSC_CarColor = 'Red' where DSC_Car = 'Seat Ibiza 1.4 TDI';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Renault Clio 1.5 DCI';
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Ford Fiesta 1.5';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Audi A3 1.6';
update [dw].[DimCar] set DSC_CarColor = 'Red' where DSC_Car = 'BMW 116i 1.6';
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Mercedes Benz C Class 1.5';
update [dw].[DimCar] set DSC_CarColor = 'Grey' where DSC_Car = 'Volkswagen Passat 2.0';
update [dw].[DimCar] set DSC_CarColor = 'Blue' where DSC_Car = 'BMW 320i Sport 2.0 Turbo';
update [dw].[DimCar] set DSC_CarColor = 'White' where DSC_Car = 'Mercedes-Benz CLS 500 5.0';
update [dw].[DimCar] set DSC_CarColor = 'Green' where DSC_Car = 'Ford Mustang GT';
update [dw].[DimCar] set DSC_CarColor = 'Red' where DSC_Car = 'Ferrari 488 GTB';
update [dw].[DimCar] set DSC_CarColor = 'Black' where DSC_Car = 'Lamborghini Aventador';
update [dw].[DimCar] set DSC_CarColor = 'Yellow' where DSC_Car = 'Renault 5 GT Turbo';
update [dw].[DimCar] set DSC_CarColor = 'Red' where DSC_Car = 'Opel Corsa A 1.5 D';
update [dw].[DimCar] set DSC_CarColor = 'Green' where DSC_Car = 'Volkswagen Lupo 1.0';
update [dw].[DimCar] set DSC_CarColor = 'Grey' where DSC_Car = 'Fiat Panda 1.2';
update [dw].[DimCar] set DSC_CarColor = 'Blue' where DSC_Car = 'Skoda Fabia 1.6';

select * from [dw].[DimCar];

rollback tran;

-- 32
begin tran;
-- a
create table DreamAirlines_DW.dw.DimMotorcycle (
	PK_Moto INT PRIMARY KEY IDENTITY(1, 1) NOT NULL,
	DSC_RentalCompany NVARCHAR(20) NULL,
	DSC_Moto NVARCHAR(50) NULL,
	Load_ID INT default 1,
	HashColumn BINARY(20) NULL,
	Insert_Date DATETIME default GETDATE(),
	Update_date DATETIME default GETDATE()
);

-- b
insert into DreamAirlines_DW.dw.DimMotorcycle(DSC_RentalCompany, DSC_Moto) 
values ('GlobalMotors', 'Harley-Davidson Street 500'),
		('GlobalMotors', 'Harley-Davidson Street 750'),
		('GlobalMotors', 'Harley-Davidson Sportster Iron 883'),
		('GlobalMotors', 'Harley-Davidson V-Rod Night'),
		('GlobalMotors', 'Harley-Davidson Dyna Fat Bob');

select * from [DreamAirlines_DW].[dw].[DimMotorcycle];

-- 33
-- a
alter table [DreamAirlines_DW].[dw].[DimCar] alter column DSC_RentalCompany NVARCHAR(20) NULL;

alter table [DreamAirlines_DW].[dw].[DimCar] alter column DSC_Car NVARCHAR(50) NULL;

EXEC sp_rename 'dw.DimCar', 'DimVehicule';

EXEC sp_rename 'dw.DimVehicule.DSC_Car', 'DSC_Vehicule'; 

select * from [DreamAirlines_DW].[dw].[DimVehicule];

-- b
insert into [DreamAirlines_DW].[dw].[DimVehicule] 
	select m.DSC_RentalCompany, m.DSC_Moto as 'DSC_Vehicule', m.Load_ID, m.HashColumn, m.Insert_Date, m.Update_date
	from [DreamAirlines_DW].[dw].[DimMotorcycle] m;

select * from [DreamAirlines_DW].[dw].[DimVehicule];

-- c
truncate table [DreamAirlines_DW].[dw].[DimMotorcycle];

-- d
drop table [DreamAirlines_DW].[dw].[DimMotorcycle];

rollback tran;

-- 34
rollback work;
