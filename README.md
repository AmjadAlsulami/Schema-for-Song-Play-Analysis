 # Schema for Song Play Analysis
## Problem Statement:
Sparkify is a startup wants to analyze the data they've been collecting a large massive data on songs and user activity on their new music streaming app. Presently, the analytics team doesn't have an easy way to  access their data and apply their analysis,  the data are  scattered into a directory of JSON logs and metadata in different places.

## Proposed Solution:
Creating a data lake in s3 and spark with tables designed to map the scattered data into star schema where the Sparkify's analysis team can apply the song play analysis, also create an ETL pipeline for this analysis and optimize queries on song play analysis. 
### How to run the Python scripts:

With the use of the song and log datasets,  **star schema** has been created. This includes the following tables.
* Fact Table
**songplays** - which contains records in log data associated with song plays and also the relation with the dimension tables it will be read from the parquet files in S3 and will be witten in parquet files too. 
* Dimension Tables
**songs** , **artists**  , **users**  , and  ***time**  tables will be wrangling from the s3 and will be written in parquet files in S3.
### Running Environment and Steps :
*  The project works on Python 3.x and to run it there is a need also to have 
* To run this project  with the use of  Amazon Web Service, 
 and to do this you need to have AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and fill the values in the file dl.cfg . Next, create an S3 Bucket named  \*dend-data-lack-work where output results will be stored , also you need to have spark by using pyspark library.

* Then, run the script  etl.py by writing \*python etl.py command.

## Explanation of the files in the repository:
- The data used in the project are can be found as [.JSON] dictionaries  in 
- s3a://udacity-dend/song_data : it's about the artist and the song.
- s3a://udacity-dend/log_data : it's about generated data by the streaming app.
- dl.cfg :add the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. 
- etl.py: the python project script.
