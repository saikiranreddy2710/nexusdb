-- Multi-database and multi-table test
CREATE DATABASE IF NOT EXISTS app1;
CREATE DATABASE IF NOT EXISTS app2;
SHOW DATABASES;
USE app1;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200);
SHOW TABLES;
USE app2;
CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT);
INSERT INTO products VALUES (1, 'widget', 10), (2, 'gadget', 20);
SHOW TABLES;
SELECT * FROM products;
USE nexusdb;
SHOW TABLES;
