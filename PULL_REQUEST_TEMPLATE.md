<!--- This template is optional. Please use it as a starting point to help guide PRs -->

<!--- Provide a general summary of your changes in the Title above -->

## Description
Changed URI.js route, modified luigi/contrib/postgres.py, create_marker_table() method adding
"IF NOT EXISTS" to the sql statements

## Motivation and Context
Luigi does not build properly, referencing issue #2028
Postgres create a error log for each query trying to create the marker table if it already exists


## Have you tested this? If so, how?
The project has passed the travis tests, luigid is running properly.
