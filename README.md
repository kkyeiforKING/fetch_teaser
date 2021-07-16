# fetch_teaser
 spark challenge

Two sets of API's are developed to dynamically generate reports based on certain parameters define by the user such as
 -> Which analysis to run
 -> How many months back to run the analysis
 -> Whether to output the results as csv, json, or parquet
 
 The API are developed using Spark DataFrame API and Spark SQL with Scala. 
 The idea here is to create a modularize code that is easy to reason about as well as 
 reusable for different Sales reports by BRAND, STORE STATE and STORE NAME.
 
 What the user is required to do to run these summary reports is to specify the Channel to get summarize sales performance, purchase duration/window, and desired output file format.
 
 I enjoyed the process! Thanks...
