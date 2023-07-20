CREATE TABLE property
(
[prop_id] INT NOT NULL,
[address] VARCHAR(100) NOT NULL,
[postcode] VARCHAR(10),
[latitude] DECIMAL(10,8) NOT NULL,
[longitude] DECIMAL(11,8) NOT NULL,
CONSTRAINT property_pk PRIMARY KEY (prop_id)
);

CREATE TABLE price
(
[prop_id] INT NOT NULL,
[date] DATE NOT NULL,
[price] INT,
CONSTRAINT price_pk PRIMARY KEY ([prop_id], [date]),
CONSTRAINT price_property_fk FOREIGN KEY (prop_id) REFERENCES dbo.property (prop_id)
);

CREATE TABLE interest_rates
(
[date] DATE NOT NULL,
[IUMSOIA] DECIMAL(6,4) NOT NULL,
CONSTRAINT interest_rates_pk PRIMARY KEY ([date])
);

CREATE TABLE SW_area_codes
(
[area_code] VARCHAR(4) NOT NULL,
CONSTRAINT SW_area_codes_pk PRIMARY KEY (area_code)
);

INSERT INTO SW_area_codes
(area_code) VALUES ('SW2'), ('SW4'), ('SW8'), ('SW9'), ('SW11'), ('SW12'), ('SW13'), ('SW14'), ('SW15'), ('SW16'), ('SW17'), ('SW18'), ('SW19'), ('SW20');

CREATE TABLE SW_postal_code 
(
postcode VARCHAR(10),
area_code VARCHAR(5),
CONSTRAINT SW_postal_codes_pk PRIMARY KEY (postcode)
);

WITH source AS 
(
	SELECT DISTINCT postcode, 
	CASE WHEN postcode LIKE '% %' THEN REPLACE(SUBSTRING(postcode, 1, CHARINDEX(' ', postcode)), ' ', '')
	ELSE REPLACE(postcode, ' ', '')
	END AS area_code
	FROM property
)
INSERT INTO SW_postal_code
(postcode, area_code)
SELECT postcode, area_code FROM source
WHERE NOT EXISTS (SELECT 1 FROM SW_postal_code t WHERE t.postcode = source.postcode)
AND EXISTS (SELECT 1 FROM SW_area_codes t2 WHERE t2.area_code = source.area_code);
