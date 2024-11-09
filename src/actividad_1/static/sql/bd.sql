
CREATE SCHEMA IF NOT EXISTS cars;

CREATE TABLE IF NOT EXISTS cars.cars_electric (
    id SERIAL PRIMARY KEY,
    Matricula VARCHAR(50),
    County VARCHAR(100),
    City VARCHAR(100),
    State VARCHAR(50),
    Postal_Code VARCHAR(20),
    Model_Year INT,
    Make VARCHAR(100),
    Model VARCHAR(100),
    f_create DATE,
    f_update DATE
);
