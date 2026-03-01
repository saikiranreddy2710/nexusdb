-- Test script for CLI/SQL commands
SHOW TABLES;
SHOW DATABASES;
CREATE TABLE IF NOT EXISTS ins_test (id INT, name TEXT);
INSERT INTO ins_test VALUES (1, 'one'), (2, 'two');
SELECT * FROM ins_test;
DESCRIBE ins_test;
