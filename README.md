# Real Time stock price forecasting using Spark Streaming and Kafka
                  /* The content of this page is not search engine indexed. */
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
