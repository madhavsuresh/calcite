select setseed(.08);

DROP TABLE IF EXISTS simple_join256_1, simple_join256_2, simple_join256_3;

CREATE TABLE simple_join256_1 (
	a_1 INT,
	b_1 INT
);

CREATE TABLE simple_join256_2 (
	a_2 INT,
	b_2 INT
);

CREATE TABLE simple_join256_3 (
	a_3 INT,
	b_3 INT
);

INSERT INTO simple_join256_1 (a_1,b_1) select floor(random()*100000+1)::int, floor(random()*1000+1)::int from generate_series(1,256) s(i);

INSERT INTO simple_join256_2 (a_2,b_2) select floor(random()*100000+1)::int, floor(random()*1000+1)::int from generate_series(1,256) s(i);

INSERT INTO simple_join256_3 (a_3,b_3) select floor(random()*100000+1)::int, floor(random()*1000+1)::int from generate_series(1,256) s(i);


CREATE ROLE public_attribute;
GRANT SELECT(a_1) ON simple_join256_1 TO public_attribute;
GRANT SELECT(a_2) ON simple_join256_2 TO public_attribute;
