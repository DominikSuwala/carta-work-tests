carta=# select * from patient;
 patient_id | mrn_id | humanname_id | birthdate  
------------+--------+--------------+------------
          1 | 020    |              | 1988-01-02
          2 | 002    |              | 2002-04-19
          3 | 003    |              | 2001-01-02
          4 | 001    |              | 1999-01-02
(4 rows)

carta=# select * from period;
 period_id |     start_date      |      end_date       
-----------+---------------------+---------------------
         1 | 2002-05-01 14:00:00 | 2002-05-02 21:00:00
         2 | 2002-04-19 19:22:00 | 2002-04-20 06:22:00
         3 | 2002-04-21 17:00:00 | 2002-04-23 02:53:00
         4 | 2002-04-12 17:00:00 | 2002-04-13 22:00:00
(4 rows)

carta=# select * from encounter;
 encounter_id | encounter_id_code | patient_id | period_id 
--------------+-------------------+------------+-----------
            1 | 0987              |          1 |         1
            2 | 2345              |          2 |         2
            3 | 3456              |          3 |         3
            4 | 1234              |          4 |         4
(4 rows)

carta=# select * from humanname;
 humanname_id | patient_id | givenname | familyname |  displayedname  
--------------+------------+-----------+------------+-----------------
            1 |          1 | Jerry     | Jones      | Jerry Jones
            2 |          2 | Joanne    | Lee        | Joanne Lee
            3 |          2 | Cosmia    | Lee        | Cosmia Lee
            4 |          3 | Annabelle | Jones      | Annabelle Jones
            5 |          4 | John      | Doe        | John Doe
(5 rows)
