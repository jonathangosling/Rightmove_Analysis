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
