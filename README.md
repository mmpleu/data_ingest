This project shows how to ingest data from JSON files to SQLite database using Pyton.

DATA ENGINEER SKILLS: EXTRACT (E) AND LOAD (L)

There are 5 JSON files in the data folder.

"run.py" extracts the data and loads it to casestudy.db file.
It also allows to query the database using command line:
python3 run.py <SQL query in quotation marks>

If no SQL is given then program runs example SQL query:
SELECT outlet.country, round(avg(reviews.rating), 2) avg_rating FROM outlet LEFT JOIN reviews ON reviews.id_outlet = outlet.id_outlet GROUP BY outlet.country

The result view is saved to outcome.csv.
