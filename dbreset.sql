DROP TABLE IF EXISTS person;
CREATE TABLE person (
    person_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    birthday DATE,
    email TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    prov TEXT,
    country TEXT,
    postcode TEXT
);

DROP TABLE IF EXISTS phone;
CREATE TABLE phone (
    phone_id INTEGER PRIMARY KEY NOT NULL,
    person_id INTEGER NOT NULL,
    number TEXT,
    label TEXT CHECK ( label IN ('WORK', 'HOME', 'CELL', 'OTHER') ),
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO person (person_id, first_name, last_name, birthday, email, address_line1, address_line2, city, prov, country, postcode)
VALUES
(1, 'Ali', 'Array', '2000-01-01', 'ali@array.com', '123 Aardvark Ave.', NULL, 'Aliston', 'ON', 'Canada', 'A2A2A2'),
(2, 'Billiy', 'Byte', '2005-05-05', 'billie@byte.com', '256 Bobber Blvd.', NULL, 'Beijing', 'Shandong', 'China', '266033'),
(3, 'Charlie', 'Char', '1969-06-13', 'charlie@char.com', '123 Charred Cresc.', NULL, 'Chesterton', 'ON', 'Canada', 'C3C3C3'),
(4, 'Dani', 'Double', '1989-09-23', 'dani@double.com', '123 Dandy Dr.', NULL, 'Durham', 'ON', 'Canada', 'D4D4D4'),
(5, 'Ernie', 'Enum', '2010-12-12', 'ernie@enum.com', '123 Ernest End', NULL, 'Erin', 'ON', 'Canada', 'E5E5E5')
;

INSERT INTO phone (person_id, number, label)
VALUES
(1, '111-111-1111', 'WORK'),
(1, '111-111-1112', 'CELL'),
(2, '222-222-2222', 'CELL'),
(2, '222-222-2345', 'WORK'),
(3, '333-333-3333', 'CELL'),
(4, '444-444-4444', 'CELL'),
(5, '555-555-5555', 'CELL'),
(5, '543-543-5432', 'HOME')
;


DROP TABLE IF EXISTS user;
CREATE TABLE user (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    password_hash TEXT
)
;

DROP INDEX IF EXISTS unique_username;
-- TODO: Create the unique_username  index
CREATE UNIQUE INDEX unique_username ON user (username);

INSERT INTO user (user_id, username, password_hash)
VALUES
(1, 'admin', 'sha256$500000$bf8ebfe3dedb22a5e87e938830313b7715ed276c$b7a4d5d3bac7965108b8060d02043d06f1b8f1454f2ea4cdd7a56cf595f569e3'),
(2, 'test', 'sha256$500000$1ca91f9ae79ac9a61cf2dc5cfaa8e46eb78233f7$9db00a708c9afe25674ee8229710e17e64a335c1d6ed7234ccb412f8dac4e822')
;