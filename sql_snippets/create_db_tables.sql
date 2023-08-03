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
[district] VARCHAR(15) NOT NULL,
CONSTRAINT SW_area_codes_pk PRIMARY KEY (area_code)
);

INSERT INTO SW_area_codes
(area_code, district) VALUES  ('SW2', 'Brixton'), 
							  ('SW4', 'Clapham'), 
							  ('SW8', 'South Lambeth'), 
							  ('SW9', 'Stockwell'), 
							  ('SW11', 'Battersea'), 
							  ('SW12', 'Balham'), 
							  ('SW13', 'Barnes'), 
							  ('SW14', 'Mortlake'), 
							  ('SW15', 'Putney'), 
							  ('SW16', 'Streatham'), 
							  ('SW17', 'Tooting'), 
							  ('SW18', 'Wandsworth'), 
							  ('SW19', 'Wimbledon'), 
							  ('SW20', 'West Wimbledon');

