# SERVER-LOG-HEALTH-ANALYSIS

### Environment

1. Scala 2.13.10
2. Spark 3.2.0
3. Intellij IDEA Community Edition
4. Apache Kafka 3.4.0
5. Apache Zookeeper 3.8.1
6. Elasticsearch 8.7.0
7. Kibana 8.7.0

### Project Description

The aim of this project is to develop a real-time data processing pipeline for server health logs to detect anomalies. By analyzing server health logs in real-time, this project aims to predict potential failures beforehand and enable system administrators to take proactive measures to prevent downtime. The analytics are displayed on a dashboard, providing a view of system performance and behavior. The project might also utilize cloud computing services for scalability, reliability, and cost-effectiveness. Overall, this project aims to provide a powerful tool for managing and optimizing system performance in real-time, enabling organizations to maximize uptime and minimize the risk of system failures.

### Use Cases

1. System sends alerts to the user if Server load is going to reach a threshold as a preventive measure
2. System analyzes the server logs to detect any anomalies
3. User monitors the server traffic activity via the visualizations on the Dashboard
4. System parses data over long term for user to determine business specific decisions such as scalability

### Execution Steps

1. Start the Zookeeper server
2. Start the Kafka server
3. Start ElasticSearch server
4. Build SBT dependencies.
5. Run main/scala/Consumer/StreamProcessing.scala
6. Run main/scala/Producer/Main.scala

### Data Source

https://www.kaggle.com/datasets/vishnu0399/server-logs
