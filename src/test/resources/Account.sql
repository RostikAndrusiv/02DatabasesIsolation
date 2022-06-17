CREATE SCHEMA IF NOT EXISTS db;

DROP TABLE IF EXISTS account;

CREATE TABLE account (
                         id 		     INT PRIMARY KEY AUTO_INCREMENT,
                         f_name 		 VARCHAR(45),
                         l_name          VARCHAR(45),
                         balance         DOUBLE,
                         PRIMARY KEY (id)
);

INSERT INTO account (f_name, l_name, balance) VALUES ('Max', 'Power', 100);