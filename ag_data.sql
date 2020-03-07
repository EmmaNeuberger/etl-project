DROP TABLE clean_customer, clean_item, clean_sales;

CREATE TABLE clean_customer(
	customerno VARCHAR,
	customertype VARCHAR);

CREATE TABLE clean_item(
	itemno VARCHAR,
	postinggroup VARCHAR,
    description VARCHAR,
	description1 VARCHAR,
	description2 VARCHAR,
	description3 VARCHAR,
	description4 VARCHAR);

CREATE TABLE clean_sales(
	itemno VARCHAR,
	customerno VARCHAR,
	yr INT,
	mo INT,
	fiscalquarter VARCHAR,
	units INT,
	sales INT);
	
SELECT * FROM clean_item LIMIT 25;