/* CREATE THE STORED PROCEDURE TO TRANSFORM AND LOAD DATA FROM DATABASE TO ANALYTICAL SCHEMA/DATA-MART */
CREATE OR ALTER PROCEDURE ETL_to_mart
AS
BEGIN
	/*
	Remove previous data
	*/
	DELETE FROM property_mart.dbo.property_fact;
	DELETE FROM property_mart.dbo.date_dim
	TRUNCATE TABLE property_mart.dbo.current_properties;

	/*
	Load new data from main database using the views
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
END;
GO