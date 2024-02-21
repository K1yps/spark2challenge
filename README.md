# Spark 2 Challenge


Response to the [Spark 2 Recruitment Challenge](https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/wiki/Spark-2-Recruitment-Challenge)

The build and program outputs are available in the [Releases](https://github.com/K1yps/spark2challenge/releases) section.

Requires Java 11

### Build

````
mvn clean compile
````

### Artifact 

#### JAR

````
mvn clean package
````

#### Über-Jar (Runnable)

````
mvn clean package -P shade
````

### Run
```
java neto.henrique.spark2challenge.App
```
or
```
java -jar .\target\spark2challenge-1.0.jar
```

### Functionality

Reads the datasets at `play-store-datasets/{googleplaystore.csv, googleplaystore_user_reviews.csv}`

**Part 1**:

* Calculates the average sentiment polarity grouped by the application name.
* Generates the DataFrame (_df1_) from `googleplaystore_user_reviews.csv`.

**Part 2**:

* Reads `googleplaystore.csv` and filters all apps with a rating greater than or equal to 4.0.
* Saves the resulting DataFrame as a single CSV, delimited by "§" at `outupt/best_apps.csv`.

**Part 3**:

* Creates a DataFrame (_df3_) by cleaning up the data from `googleplaystore.csv`

**Part 4**:
* Combines the DataFrames produced by Parts 1 and 3 into a new Dataframe (_df4_).
* Saves the final DataFrame as a single Parquet file with gzip compression at `outupt/googleplaystore_cleaned`.

**Part 4**:
* Utilizes _df4_ to create a new DataFrame (_df5_) containing metrics like the count of applications, average rating, and average sentiment polarity by genre.
* Saves the resulting DataFrame as a single Parquet file with gzip compression at `outupt/googleplaystore_metrics`.



