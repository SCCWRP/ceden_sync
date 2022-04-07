# CEDEN to SMC Data Sync Routine
I think it should work like this
1) pull all ceden data into their own tables
2) create complex views that look like the unified tables (using as and string functions to match them up)
3) haul them into unified tables with SQL, using "UPSERT" statements. These SQL commands may have to be generated with python, then executed with eng.execute. in this case we can still 
capture the error message from the database if there is a problem, so that is still ok (unlike when we do it with psql)


