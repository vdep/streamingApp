# Real Time stock price forecasting using Spark Streaming and Kafka

#Architecture
<p align="center">
  <img src = "https://github.com/vdep/streamingApps/blob/main/lib/Architecture.png"/>
</p>
#Input
Input consists of multiple sources including API calls which returns stock data in JSON format and shell scripts to read data from files and databases. Each input source has different streaming intervals.
#Kafka & Spark
The input sources are ingested by single node kafka cluster to maintain the order of the data and for durability.

The output from kafka is fed to spark for computation. Since the streaming interval is different for different input sources, quotes for some keys may be more frequent than others. Hence the keys are aggregated to maintain a uniform stream interval and stored in a Cassandra table, this aggregated data is used for forecasting.
#Forecasting
Autoregressive integrated moving average (ARIMA) model from spark-ts library, which was previously trained on historic data is used for forecasting. The prediction is done for every next immediate time interval. Some algorithms in spark MLlib has .trainOn() method which updates the existing model with the new data. The same process is reproduced here by manually retraining the model after every n streaming intervals. The accuracy of the model is monitored and is used as an indicator along with streaming interval count for model retraining.

The forecasted value is stored in another Cassandra table along with percentage change in the value and trend, when compared to previous interval. This data can be visualized using D3.js in both real time and batch mode.  

output from Cassandra tables containing aggregated and forecasted data:

     cqlsh> select * from test.averaged;
    
     uniquekey | avg      | count | name | sum       | timestamp
    -----------+----------+-------+------+-----------+---------------
       1976270 | 34.30437 |     3 | ALTR |  102.9131 | 1473075681000
        268479 | 51.18375 |     2 |  ACN |  102.3675 | 1473075681000
       1192084 |  34.6661 |     3 | ALTR |  103.9983 | 1473075684000
        .
        .

    cqlsh> select * from test.prediction;
    
     uniquekey | forecastvalue | percentagechange | trend
    -----------+---------------+------------------+----------
       1034048 |      50.39514 |         -1.551 | decrease
        796357 |      50.28032 |        -1.7521 | decrease
        710180 |      51.05806 |        -0.2493 | decrease
       1002066 |      51.61904 |         0.8482 | increase
        .
        .
        
Tool | Version
:---: | :---:
Spark | 1.6.0
Scala | 2.10.5
Cassandra | 2.2.5
Kafka | 0.10.0.0
Cassandra Connector | 1.6.0-M2
Kafka Connector | 1.3.0
Sparkts | 0.13
