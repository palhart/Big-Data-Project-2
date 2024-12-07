from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.functions import col, when, to_timestamp, month, year, count, avg, expr
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

class CustomerSegMLAnalysis:
    def __init__(self, spark, df):
        self.spark = spark
        
        # Convert string columns to appropriate types
        for col_name, col_type in [
            ('timestamp', 'timestamp'), 
            ('price', 'double'), 
            ('quantity', 'int'), 
            ('total_amount', 'double')
        ]:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
        
        self.df = df
        
    def _prepare_features(self, df, feature_columns):
        # Identify numeric and categorical columns
        numeric_cols = []
        categorical_cols = []
        for col_name in feature_columns:
            col_type = df.dtypes[df.columns.index(col_name)][1]

            if col_type in ['int', 'double', 'float']:
                numeric_cols.append(col_name)
            else:
                categorical_cols.append(col_name)
        
        # String Indexing for categorical columns
        indexers = [StringIndexer(inputCol=category, outputCol=f"{category}_index", handleInvalid="keep") 
                    for category in categorical_cols]
        
        # One-Hot Encoding for indexed categorical columns
        encoders = [OneHotEncoder(inputCol=f"{category}_index", outputCol=f"{category}_encoded") 
                    for category in categorical_cols]
        
        # Combine all features
        feature_cols = numeric_cols + [f"{category}_encoded" for category in categorical_cols]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Feature scaling
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Create pipeline
        pipeline_stages = indexers + encoders + [assembler, scaler]
        pipeline = Pipeline(stages=pipeline_stages)
        
        return pipeline, "scaled_features"
    
    def _calculate_customer_metrics(self):
        # Calculate customer metrics
        customer_metrics = self.df.groupBy("customer_id", "customer_name", "city", "customer_type") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            avg("total_amount").alias("avg_transaction_amount"),
            F.sum("total_amount").alias("total_purchase_amount"),
            F.max(to_timestamp(col("timestamp"))).alias("last_purchase_date"),
            F.countDistinct("product_name").alias("unique_products_purchased"),
            F.countDistinct("category").alias("unique_categories"),
            F.count("transaction_id").alias("purchase_frequency"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.sum(F.when(col("category") == "specific_category", col("total_amount")).otherwise(0)).alias("spending_in_specific_category"),
            F.percentile_approx("total_amount", 0.25).alias("spending_25th_percentile"),
            F.percentile_approx("total_amount", 0.5).alias("spending_50th_percentile"),
            F.percentile_approx("total_amount", 0.75).alias("spending_75th_percentile"),
            F.first("product_name").alias("top_product"),
            F.countDistinct("product_name").alias("product_diversity"),
            F.countDistinct("category").alias("category_engagement"),
            F.count("category").alias("category_frequency"),
            F.sum("total_amount").alias("customer_lifetime_value"),
            F.variance("total_amount").alias("spending_variance"),
            F.expr("percentile(total_amount, 0.5)").alias("median_spend")
        )
        
        # Calculate recency (days since last purchase)
        customer_metrics = customer_metrics.withColumn(
            "recency_days", 
            F.datediff(F.current_date(), F.to_date(col("last_purchase_date")))
        )
        
        return customer_metrics
    
    def cluster_customers(self, k=5):
        # alculate Customer Metrics
        customer_metrics = self._calculate_customer_metrics()
        
        #   Prepare Features for Clustering
        feature_columns = [
            "top_product", 
            "spending_variance", 
        ]

        pipeline, feature_col = self._prepare_features(customer_metrics, feature_columns)
        
        # Fit the pipeline and transform the data
        model = pipeline.fit(customer_metrics)
        features_df = model.transform(customer_metrics)
        
        # Train K-Means Model
        kmeans = KMeans(featuresCol=feature_col, predictionCol="cluster", k=k, seed=42)
        kmeans_model = kmeans.fit(features_df)
        
        # Evaluate Clustering
        evaluator = ClusteringEvaluator(featuresCol=feature_col, predictionCol="cluster", metricName="silhouette")
        silhouette_score = evaluator.evaluate(kmeans_model.transform(features_df))
        
        # Assign clusters to customers
        clustered_customers = kmeans_model.transform(features_df)
        
        return clustered_customers.select(
            "customer_id", 
            "customer_name", 
            "cluster", 
            *feature_columns
        ), silhouette_score
