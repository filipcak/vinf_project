# VINF project

Seminar project for VINF course.
 

## Spark command to run project
This command will run spark job with xml parser of one wikipedia dump file
Or it will run simple parser with procyclingstats data or it will run merger of both data sets.
```
spark-submit --packages com.databricks:spark-xml_2.12:0.13.0 --driver-memory 27G parser_spark.py p procyclingstats
arg1: p - parse, m - merge, else - exit
arg2: procyclingstats - parse procyclingstats, wiki - parse wiki, else - exit
```

## Indexing with Lunece

Need to run docker with VS Code and open project in it. 

```
indexer_lucene.py
```
For local run use this command:
```
usr/local/bin/python /workspaces/vinf_project/indexer_lucene.py
```