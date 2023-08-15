/* NOTE: we only need to create the views once! Tables can be dropped, recreated and repopulated 
using the views each time we want to refresh the data in the mart from the database */
-----------------------------------------------------------------------------------------------------------------------------
/*
Create view to calculate the median and mean price for each date, categorised by area code
*/
CREATE OR ALTER VIEW property_area_fact_view
AS
-- first cte to extract the area code from the postcode (CASE statement used as some prostcodes are already just the area code)
WITH area_code_cte AS
(
SELECT price.[date], 
CASE WHEN prop.postcode LIKE '% %' THEN REPLACE(SUBSTRING(prop.postcode, 1, CHARINDEX(' ', prop.postcode)), ' ', '')
	 ELSE REPLACE(prop.postcode, ' ', '')
END AS area_code,
price.price
FROM properties.dbo.price price
JOIN properties.dbo.property prop
ON price.prop_id = prop.prop_id
), 
-- second cte to calculate the median price (using the percentile_cont function, we seem to need to do this before aggregating)
median_cte AS
(
SELECT 
[date], 
area_code, 
price, 
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price ASC) OVER (PARTITION BY [date], area_code) AS median_price
FROM area_code_cte
-- check that the area code is within our list of SW area codes, if not then we don't want to include it
WHERE EXISTS (SELECT 1 FROM properties.dbo.SW_area_codes t2 WHERE t2.area_code = area_code_cte.area_code)
)
-- finally, group by the date and area code and aggregate the price (median already aggregated above, just MIN to get the value)
SELECT 
[date], 
area_code, 
CAST(AVG(price) AS INT) avg_price, 
CAST(MIN(median_price) AS INT) median_price, 
COUNT(*) AS num_properties
FROM median_cte
GROUP BY [date], area_code;
GO

/*
Create view to calculate the median and mean price for each date for all properties
*/
CREATE OR ALTER VIEW property_all_fact_view
AS
-- calculate the median price (using the percentile_cont function, we seem to need to do this before aggregating)
WITH median_cte AS
(
SELECT 
price.[date], 
price.price, 
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price.price ASC) OVER (PARTITION BY price.[date]) AS median_price
FROM properties.dbo.price price
JOIN properties.dbo.property prop
ON price.prop_id = prop.prop_id
)
-- group by the date and area code and aggregate the price (median already aggregated above, just MIN to get the value)
-- we add a column for 'area_code' specifying 'all'. This means we can join with the area code aggregated table so that in
-- analysis we can filter by specific area codes, or by all area codes, or just show everything!
SELECT 
[date], 
'all' area_code, 
CAST(AVG(price) AS INT) avg_price, 
CAST(MIN(median_price) AS INT) median_price, 
COUNT(*) AS num_properties
FROM median_cte
GROUP BY [date];
GO

/*
Join all aggregated properties view to area-specific view
*/
CREATE OR ALTER VIEW property_fact_view
AS
SELECT * FROM property_area_fact_view
UNION
SELECT * FROM property_all_fact_view;
GO

/* 
Create view for dates 
*/
CREATE OR ALTER VIEW date_dim_view
AS
WITH dates AS
(
SELECT DISTINCT([date]) [date] FROM properties.dbo.price
)
SELECT [date], DATENAME(MONTH, [date]) [month], DATENAME(YEAR, [date]) [year]
FROM dates;
GO

/*
Create view for snapshot of current properties
*/
CREATE OR ALTER VIEW current_properties_view 
AS
WITH area_code_cte AS
(
SELECT
prop.prop_id,
prop.[address],
prop.postcode,
prop.latitude,
prop.longitude,
CASE WHEN prop.postcode LIKE '% %' THEN REPLACE(SUBSTRING(prop.postcode, 1, CHARINDEX(' ', prop.postcode)), ' ', '')
	 ELSE REPLACE(prop.postcode, ' ', '')
END AS area_code,
price.price,
price.[date]
FROM properties.dbo.price price
JOIN properties.dbo.property prop
ON price.prop_id = prop.prop_id
-- only select the most recent data
WHERE price.[date] = (SELECT MAX([date]) FROM properties.dbo.price price)
)
SELECT
prop_id,
[address],
postcode,
latitude,
longitude,
-- only include the area code data when the area code is in SW London
CASE WHEN area_code IN (SELECT t.area_code FROM properties.dbo.SW_area_codes t) THEN area_code
	 ELSE NULL
END AS area_code,
price,
[date]
FROM area_code_cte;
GO

DROP TABLE IF EXISTS property_mart.dbo.property_fact;
/* 
Create static area code table
*/
CREATE TABLE property_mart.dbo.area_dim
(
[area_code] VARCHAR(4) NOT NULL,
[district] VARCHAR(15) NOT NULL,
 CONSTRAINT area_dim_pk PRIMARY KEY ([area_code])
)

INSERT INTO property_mart.dbo.area_dim
SELECT * FROM properties.dbo.SW_area_codes;

INSERT INTO property_mart.dbo.area_dim
([area_code], [district]) VALUES ('all', 'all districts');

/* 
Create tables varying (to be updated with time-varying data) tables
*/
CREATE TABLE property_mart.dbo.date_dim
(
[date] DATE NOT NULL,
[month] VARCHAR(10) NOT NULL,
[year] INT NOT NULL,
CONSTRAINT date_dim_pk PRIMARY KEY ([date])
);

CREATE TABLE property_mart.dbo.property_fact
(
[date] DATE NOT NULL,
area_code VARCHAR(4) NOT NULL,
avg_price INT,
median_price INT,
num_properties INT,
CONSTRAINT property_fact_pk PRIMARY KEY ([date], area_code),
CONSTRAINT property_fact_date_dim_fk FOREIGN KEY ([date]) REFERENCES property_mart.dbo.date_dim ([date]),
CONSTRAINT property_fact_area_dim_pk FOREIGN KEY ([area_code]) REFERENCES property_mart.dbo.area_dim ([area_code])
)

DROP TABLE IF EXISTS property_mart.dbo.current_properties;
CREATE TABLE property_mart.dbo.current_properties
(
prop_id INT NOT NULL,
[address] VARCHAR(100),
postcode VARCHAR(10),
latitude DECIMAL(10,8) NOT NULL,
longitude DECIMAL(11,8) NOT NULL,
area_code VARCHAR(4),
price INT,
[date] DATE NOT NULL,
CONSTRAINT current_properties_pk PRIMARY KEY (prop_id)
)

/*
Load data from main database using the views
*/
INSERT INTO date_dim
([date], [month], [year])
SELECT
[date], [month], [year]
FROM date_dim_view;

INSERT INTO property_fact
([date], area_code, avg_price, median_price, num_properties)
SELECT
[date], area_code, avg_price, median_price, num_properties
FROM property_fact_view;

INSERT INTO current_properties
(
prop_id, [address], postcode,
latitude, longitude, area_code, 
price, [date]
)
SELECT
prop_id, [address], postcode,
latitude, longitude, area_code, 
price, [date]
FROM current_properties_view;
