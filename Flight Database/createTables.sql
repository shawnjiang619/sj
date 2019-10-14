CREATE TABLE USERS (
  username VARCHAR(100) PRIMARY KEY,
  hash VARBINARY(100),
  salt VARBINARY(100),
  balance INT
);

CREATE TABLE Reservations (
  rid INT PRIMARY KEY,
  paid int, -- 1 means paid
  username VARCHAR(100) REFERENCES USERS,
  fid1 int REFERENCES FLIGHTS, -- Itineraries
  fid2 int REFERENCES FLIGHTS, -- Itineraries
  price int,
  day int
);

CREATE TABLE ReservationID (
  rid INT PRIMARY KEY
);

INSERT INTO ReservationID VALUES(1);
