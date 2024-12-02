java c
CIS 5450 Homework   2:   SQL   and   DuckDB
Due:   Friday, October   11 2024,   10:00pm   EST   Worth 95 points in total (25 manually graded)
Welcome to Homework 2! By now, you should be familiar with the world of data science and the Pandas   library. This assignment focuses on   helping you get to grips with two new tools: SQL and DuckDB.
Through this homework, we will be working with SQL by exploring a Indego   dataset containing bike rides, stations and weather data. We will   then   expand   our   exploration   with   DuckDB   and   inish   by   comparing   the   two   with   Pandas.
We are introducing a lot of new things in this homework, and this is   often where   students   start to get   lost. Thus, we strongly encourage you to   review   the   slides/material   as   you   work   through   this   assignment.
Before you begin:
·       Be sure to click "Copy to Drive" to make sure you're working on your own personal version of the homework
·      Check the pinned FAQ post on Ed for updates!   If you   have   been stuck,   chances   are   other   students   have   also faced   similar   problems.   Y       Part 0: Libraries   and   Set   Up Jargon
import      pandas      as      pd!pip3      install      penngrader-client   !pip      install      sqlalchemy==1.4.46   !pip      install      pandasql
!pip      install      geopy
!pip      install      -U      kaleido
from      penngrader.grader      import      *
import      pandas      as      pd
import      datetime      as      dt
import      geopy.distance      as      gp
import      matplotlib.image      as      mpimg   import      plotly.express      as      px
#      import      re
import      pandasql      as      ps      #SQL      on      Pandas      Dataframe
import      nltk
nltk.download('punkt')   import      duckdb
from      wordcloud      import      WordCloud
from      matplotlib.dates      import      date2num   import      matplotlib.pyplot      as      plt
from      PIL      import      Image
#      from      collections      import      Counter
#      import      random
#      Three      datasets      we're      using
!      wget      -nc      https://storage.googleapis.com/penn-cis5450/indego_trips.csv
!      wget      -nc      https://storage.googleapis.com/penn-cis5450/indego_stations.csv
!      wget      -nc      https://storage.googleapis.com/penn-cis5450/weather_2022_PHL.csv
PennGrader Setup
#      PLEASE      ENSURE      YOUR      PENN-ID      IS      ENTERED      CORRECTLY.      IF      NOT,      THE      AUTOGRADER      WON'T      KNOW
#      TO      ASSIGN      POINTS      TO      YOU      IN      OUR      BACKEND
STUDENT_ID      =            #      YOUR      PENN-ID      GOES      HERE      AS      AN      INTEGER      #
SECRET      =      STUDENT_ID
%%writefile      config.yaml
grader_api_url:      'https://23whrwph9h.execute-api.us-east-1.amazonaws.com/default/Grader23'
grader_api_key:      'flfkE736fA6Z8GxMDJe2q8Kfk8UDqjsG3GVqOFOa'
grader      =      PennGrader('config.yaml',      'cis5450_fall24_HW2',      STUDENT_ID,      SECRET)


Biking in   Philadelphia
   I'm sure in your time in Philadelphia so far you've come across these blue bikes and stations.   Indego   is the company   responsible for this bike         sharing ride system, and they make data on bike trips available to the public. This data can not only be   useful to get   information   of   how   people   in Philly use bikes, but it can give   information on the   most visited places   in the city which can   be   useful for   city   planners   and   business   owners.
In this homework, we'll be exploring some data about bikes including:         ·      Trips: data   about   bike   trips   during   theirst   week   of   October   2022.   ·      Stations: data about bike stations, their ID and   Name.
·      Weather: data about the weather in Philadelphia during 2022.
We'll be parsing this data into dataframes and relations, and then exploring how to query and   assemble the tables into   results. We will   primarily   be using DuckDB, but for some of the initial questions, we will ask you to perform. the same operations in   Pandas as well,   so   as to familiarize you   with   the   differences   and   similarities   of   the   two.
Part 1: Load    Process our   Datasets   [15   points total]   Before we get into the data, we irst need to load and clean our datasets.
Metadata
You'll be working with three CSV iles:
      indego_trips.csv
      indego_stations.csv            weather_2022_PHL.csv
The   ile   indego_trips.csv   contains   data   about   each   trip, like   the   origin   station, destination   station   and   duration.   The   ile   indego_stations.csv   includes   information   about   stations   and   their   status   in   January   2023.
The ile   weather_2022_PHL.csv   has one row per day during 2022 and shows weather information. TODO:
·       Load    indego_trips.csv   and save the data to a dataframe. called    trips_df   .
·       Load    indego_stations.csv   and save the data to a dataframe. called    stations_df   .
·       Load   weather_2022_PHL.csv   and save the data to a dataframe. called   weather_df   .
#      TODO:      Import      the      datasets      to      pandas      dataframes      --      make      sure      the      dataframes      are      named      correctly!
#      view      trips_df      using      .head()      to      make      sure      the      import      was      successful
#      view      stations_df      using      .head()      to      make      sure      the      import      was      successful
#      view      weather_df      using      .head()      to      make      sure      the      import      was      successful


1.1 Data   Preprocessing
Next, we are going to want to clean up   our dataframes, namely    trips_df   and    stations_df   , by 1) ixing columns, 2) changing datatypes, 3)   handling nulls.
First, let   us   view   theirst   few   rows   of      trips_df   . You   may   also   call      .info()   and   additionally   check   the   cardinality   of   each   column   to   view   the speciics   of   the   dataframe. This   is   a   good   irst   step   to   take   for   Exploratory   Data   Analysis   (EDA).
1.1.1 Cleaning   trips_df   [8   points]
.info()   gives us meaningful information regarding columns, their types, and the amount of nulls,   based on which we can   now clean   our   dataframe.
Perform. these steps and save results on a new dataframe.    trips_cleaned_df   TODO:
·       Drop the column   plan_duration   . We already have that information in the column    passholder_type   , which is more understandable.   ·       Drop the rows where    end_station   is 3000. This is a virtual station used for maintainance, and   doesn't   represent   a   real trip.
·       Drop all rows with null values.   ·      Cast the columns:
o          start_time   ,   end_time   ,   trip_route_category   ,   passholder_type   ,   bike_type   as string. (Cast to 'string'and not 'str')         o         bike_id   as   int.
·       Sort results by    trip_id   ascending
·       Reset and drop the index and save results   as    trips_cleaned_df
After performing these steps,   trips_cleaned_df   should have the following schema:   Final Schema:
trip_id          duration          start_time          end_time          start_station          start_lat          start_lon          end_station          end_lat          end_lon          bike_id          trip_route_category          passholder_type          bike_type
#view      info      of      trips_df
#      TODO:      drop      plan_duration
#      TODO:      drop      rows      with      irrelevant      end_station
#      TODO:      drop      rows      with      null      values
#      TODO:      cast      the      types      of      the      columns
#      TODO:      sort      the      results
#      TODO:      drop      the      index      and      save      the      results
#      4      points
grader.grade(test_case_id      =      'test_cleaning_trips',      answer      =      trips_cleaned_df)
Now we are going to clean up the start_time and end_time columns so that they are easier to use. We will be using   Regex   in this section to
separate out the date and the time from the entries.   TODO:
·       Fill in the Regex patterns to retrieve irst the date, and then the time, found in each entry.
·       Extract the relevant parts of each column and   populate   new columns   called the following:    date_start   ,    time_start   ,    date_end   ,    time_end   .      Note that the datetime type does contain both date and time, but that we are wanting to explore your Regex capabilities :)
·       Cast the new date columns as    datetime64[ns], using pd.to_datetime() and the format as '%m/%d/%Y'   ·       Remove columns    start_time   ,    end_time


#view      trips_cleaned_df      using      .head()
#      TODO:      cast      the      types      of      start_time      and      end_time      to      'string'
#      TODO:      fill      in      the      pattern      to      retrieve      the      date      of      each      entry
#      HINT:      think      about      the      unique      syntax      of      the      date      section,      the      different      variations      it      can      appear      in
date_pattern      =
#      TODO:      fill      in      the      pattern      to      retrieve      the      time      of      each      entry
#      HINT:      think      about      the      unique      syntax      of      the      time      section,      the      different      variations      it      can      appear      in
time_pattern      =
#      TODO:      populate      columns      date_start,      time_start,      date_end,      time_end
#      TODO:      cast      the      date      columns      to      as      datetime64[ns]
#      TODO:      drop      the      start_time      and      end_time
#      4      points
grader.grade(test_case_id      =      'test_regex_trips',      answer      =      trips_cleaned_df)
1.1.2 Processing Stations [3   Points]
stations_df   contains   information   on   Indego   stations   across   the   city. We   will   clean   this   df   by   removing   Inactive   stations   and   stations   created after October 2022.
Perform. these steps and assign the cleaned dataframe. to   stations_cleaned_df   .   TODO:
·       Drop the stations that have an   Inactive status.
·       Cast column    day   of   go   live_date   as datetime64[ns].
·      Drop the stations that were created after 10/7/2022 since this is the last date of rides we are analyzing.   
·       Drop the columns    day   of   go   live_date   and    status
·       Create a new column called    is_west_philly   that is True if zone   is 2   or   3   and   False   otherwise.
·       Save the resulting dataframe. as    stations_cleaned_df   , and sort it by    station_id   ascending
After performing these steps,   stations_cleaned_df   should have the following schema:   Final Schema:
station_id          station_name          zone          is_west_philly   
#view      info      of      stations_df
#      TODO:      Drop      the      stations      that      have      an      Inactive      status.
#      TODO:      Cast      column      day_of_go_live_date      as      datetime64[ns].
#      TODO:      Drop      the      stations      that      were      created      after      10/7/2022.
#      TODO:      Drop      day_of_go_live_date      and      status      columns
#      TODO:      Create      a      new      column      called      is_west_philly      that      is      True      if      zone      is      2      or      3      and      False      otherwise.
#      TODO:      Sort      by      station_id      ascending
#      TODO:      Reset      and      drop      the      index,      and      save      the      resulting      dataframe      as      stations_cleaned_df


#      3      points
grader.grade(test_case_id      =      'test_cleaning_stations',      answer      =      stations_cleaned_df)
Y       1.1.3 Cleaning the weather [4 Points]
Then, let's clean   weather_df   and make it usable. We are   going to   make two different datasets, one for the actual   data,   and   another   for the   record-holding data.
TODO:
·       Create    actual_weather_cleaned_df   and only keep the following 5代 写CIS 5450 Homework 2: SQL and DuckDBPython
代做程序编程语言 columns:
o          date   ,   actual_mean_temp   ,   actual_min_temp   ,   actual_max_temp   ,   actual_precipitation      ·       Create   record_weather_cleaned_df   and only keep the following 4 columns:
o          date   ,   record_min_temp   ,   record_max_temp   ,   record_precipitation      Then   for   both   datasets:
·       Convert column    date   into type    datetime64[ns]   .
·         Keep   only   the   rows   from   9/1/2022 to   10/31/2022, inclusive.   ·       Sort by column    date   descending.
·       Reset and drop the index.
After performing these steps,   actual_weather_cleaned_df   should have the following schema:   Final Schema:
date         actual_mean_temp          actual_min_temp          actual_max_temp          actual_precipitation
... and   record_weather_cleaned_df   should have the following schema:   Final Schema:
date          record_min_temp          record_max_temp          record_precipitation   
#view      info      of      weather_df
#      TODO:      create      actual_weather_cleaned_df
#      TODO:      create      record_weather_cleaned_df
#      TODO:      for      both      datasets,      convert      column      'date'      into      type      datetime64[ns]
#      TODO:      for      both      datasets,      keep      only      the      rows      from      9/1/2022      to      10/31/2022,      inclusive
#      TODO:      for      both      datasets,      sort      by      column      'date'      descending
#      TODO:      for      both      datasets,      reset      and      drop      the      index
#      4      points
grader.grade(test_case_id      =      'test_cleaning_weather',      answer      =      [actual_weather_cleaned_df,      record_weather_cleaned_df])
Part 2: DuckDB   [55   points total]
IMPORTANT: Pay VERY CLOSE   attention to this style   guide!   命命命
The   typical   low   to   use   duckdb   is   as   follows:
1. Write a SQL query in the form. of a   string
。   String Syntax: use triple quotes    """"""   to write multi-line strings
。Aliases are your friend: if there are very long table names   or you ind yourself needed to declare the source (common during   join   tasks), it's almost always optimal to alias your tables with short INTUITIVE alias names
o         New Clauses New Line: each of the main SQL clauses   ( SELECT   ,    FROM   ,   WHERE   , etc.) should begin   on   a   new   line               o         Use Indentation: if there are many components for a single clause, separate them out with   new   indented   lines.
Example below:
"""
SELECT      ltn.some_id,      SUM(stn.some_value)      AS      total
FROM      long_table_name      AS      ltn
INNER      JOIN      short_table_name      AS      stn
ON      ltn.common_key      =      stn.common_key   INNER      JOIN      med_table_name      AS      mtn
ON      ltn.other_key      =      mtn.other_key
WHERE      ltn.col1      >      value
AND      stn.col2      <=      another_value
AND      mtn.col3      !=      something_elseGROUP   ORDER   """
ltn.some_id   total
2.   Run the query using duckdb.sql(your_query)
Duckdband   pandasql   are   convenient   in   that   they   allow   you   to   reference   the   dataframes   that   are   currently   deined   in   your   notebook, so   you   will   be able to fully utilize the dataframes that you have created above!
Given that it is a brand new language, we wanted to give you a chance to directly compare the similarities/differences of the pandas that you   already know and the SQL that you are about to learn. SQL queries may take up to a   minute to run.
ΔWARNING: DO NOT USE PANDAS   FOR SQL   QUESTIONS   OR VICE-VERSA!   OTHERWISE,   YOU   WON'T   RECEIVE   CREDITS   FOR THESE   QUESTIONS
#TODO:      Run      this      cell      to      understand      how      Duck      DB      connects      to      local      dataframes      and      queries      them
test_duckdb_query      =      """
SELECT      *
FROM      actual_weather_cleaned_df   LIMIT      10
"""
duckdb.sql(test_duckdb_query)
2.1 Comparing Rides by   Passholders [17   points]
2.1.1 How many rides were taken per passholder type? [3   points]
The dataframe   trips_cleaned_df   contains information for each ride. We want to know which of these rides were taken per passholder type (Day   Pass, Indego30, Indego365), and how many   such   rides were taken.
TODO:
·      Create a DuckDB query that counts the number of trips taken per passholder type.
·      The resulting table should have the column   passholder_type   that contains the pass names and another column    trip_count   that is the   count   of   the   number   of   trips   taken   on   that   pass.
·       Sort this table by   trip_count   in descending order.
Note: Do not modify the lines where we are calling the Duck   DB SQL   engine,   as this   line converts your   results to a   Pandas   dataframe. for   our test   cases.
Hint: It would be helpful to use the    GROUP   BY   and    ORDER   BY   statements in SQL. Documentation for the    GROUP   BY   statement can be found here   and   ORDER   BY   statement can be found here.
Final Schema:
passholder_type         trip_count   
#      TODO:      Use      Duck      DB      to      get      the      total      number      of      trips      by      passholder      typ
passholder_count_query      =      """
"""
passholder_count      =      duckdb.sql(passholder_count_query).df()
#      3      Points
2.1.2 What was the average distance between the start and endpoint of the ride by passholder type? [5 points]   We are now interested in what the average displacement was between the start and end of the ride   by   each   passholder type.
TODO:
·       Create   a   (Python) function   called      calculate_distance   that   returns   the   distance   in   miles   between   two longitude/latitude coordinates. The input parameters should be    start_lat   ,    start_lon   ,    end_lat   ,   end_lon
·       Create a function in Duck DB using the      create_function   functionality in   Duck   DB.   Name this function    distance   . The input/output datatypes   for   this   function   should   all   be   float   .
·      Use this function inside a Duck DB query to ind the average trip displacement by passholder type for non-roundtrip   rides from   trips_cleaned_df   .
·      Sort the table by the average displacement in descending order.
Documentation on using Python functions in Duck   DB can   be found here. The   "Python   Function API" and   "Creating   Functions"   sections would   be   most   helpful.
Hint: Use the GeoPy Distance module to help create your function, which has already been   imported for you   at the top of this notebook!   Documentation on this module can be found here.
Final Schema:
passholder_type          avg_displacement   
#      TODO:      Finish      the      rest      of      the      calculate_distance      function      by      using      the      GeoPy      Distance      module.
def      calculate_distance(start_lat,      start_lon,      end_lat,      end_lon):
pass
The following test case is not graded but given to you all as a spot check.   If you don't receive   a   hint from the test case you are good to go!
#      0      point
grader.grade(test_case_id      =      'test_distance_function',      answer      =      calculate_distance(30.4515,      -91.1871,      29.9509,      -90.0758))
#      DO      NOT      EDIT
try:
duckdb.remove_function("distance")   except:
pass
#      TODO:      Use      the      calculate_distance      function      to      create      a      Duck      DB      function      with      the      correct      parameters
#      TODO:      Use      Duck      DB      to      find      the      average      displacement      for      non-roundtrip      rides      for      each      passholder      type.
distance_query      =      """
"""
passholder_distance      =      duckdb.sql(distance_query).df()
passholder_distance
#      5      points
grader.grade(test_case_id      =      'test_passholder_distance',      answer      =      passholder_distance)
2.1.3 What is the distribution of electric vs standard bikes for each type of pass? [4 points]
TODO:
·       Create a DuckDB query that inds the percentage of rides that used an electric bike for each type of pass from    trips_cleaned_df   .
·      The resulting table should have the column   passholder_type   and    electric_pct   ,   which shows what percentage of the rides taken for that   pass were on electric bikes.
·       For the    electric_pct   column, scale the percentages to be between 0 and   100 (e.g. 40.4%   = 40.4)   and   rounded to   1 decimal   place.   ·       Sort the table by   electric_pct   in descending order.
·       Do not modify the variable name of the query string (passholder_bike_type_query), as this variable will be passed into the test case.
Hint: For documentation on rounding in SQL, see the documentation here. For documentation   on the    CASE   statement, see the documentation   here.
Final Schema:
passholder_type          electric_pct   
#      TODO:      Create      a      Duck      DB      query      that      finds      the      percentage      of      rides      on      an      electric      bike      for      each      type      of      passholder.
passholder_bike_type_query      =      """
"""
passholder_bike_type      =      duckdb.sql(passholder_bike_type_query).df()
passholder_bike_type
#      4      points
grader.grade(test_case_id      =      'test_passholder_bike_type',      answer      =      (passholder_bike_type,      passholder_bike_type_query))
2.1.4 What are the most popular rides for each day of the week? [4   points]
We want to understand what are the most popular days of the week for bike rides for each type of pass in    trips_cleaned_df   .   TODO:
·      Create a query using Duck DB that returns a table with the following schema:
Day of Week          Number of Days          Day Pass          Indego30         Indego365
·      The   Day   of   Week   column indicates the speciic day of the week when the rides occurred. The   Day   Pass   ,    Indego30   , and    Indego365   columns                  represent the total number of rides for each type of pass on that particular day of the week. For example, if a   row   lists   "Monday"   in the   Day   of Week column, the value in the Day Pass column should relect the total number   of rides taken with a   Day   Pass on all   Mondays
throughout   the   dataset.
·       Number   of   Days   is the number   of   dates   in   our   dataset that fall on that row's   day   of the week. ·      Sort the dataframe. to start from Monday and end on Sunday.
Hint: Look at the documentation here   to extract day of week names in SQL. Look at the documentation here   to igure out how to count the   distinct dates for each day of the week in the   Number   of   Days   column.
Hint: If we are deining a column alias in SQL   as   a string with   spaces   (e.g.   "Day   of Week"), we   have to   put   it   in double   quotes   inside   of the query.   To reference a string, only single quotes are needed   (e.g. 'Monday').
Hint: To sort the dataframe. by the correct days of the week, consider using a CASE statement   to assign each day of the week to a number, then   sorting   by   this   assignment.
#      TODO:      Create      a      Duck      DB      query      in      the      schema      specified      above      that      counts      the      number      of      bike      rides      per      passholder      type      on      ever
day_of_week_query      =      """
"""
day_of_week      =      duckdb.sql(day_of_week_query).df()
day_of_week   
#      4      points
grader.grade(test_case_id      =      'test_day_of_week_count',      answer      =      day_of_week)
Relect: What do you think? [1 point]
Which day of the week did Indego30 passholders ride the most? What can you say about the distribution of the number of   rides for each day   of   the week? Do we have enough data to draw a fair conclusion about our data or   rider   behavior?
#      TODO:      Answer      the      above      reflection      questions.
your_answer      =      "[Your      answer      here]"
#      1      point
grader.grade(test_case_id      =      'test_reflection',      answer      =      your_answer)
   

         
加QQ：99515681  WX：codinghelp  Email: 99515681@qq.com
