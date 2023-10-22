DROP TABLE IF EXISTS canada_exchange_rates 
CREATE TABLE canada_exchange_rates(
				dates timestamp,
				fxusdcad_rates decimal, 
				fxeurcad_rates decimal, 
				fxaudcad decimal)
SELECT * FROM canada_exchange_rates;