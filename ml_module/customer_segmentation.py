from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.ml.pipeline import Pipeline

class CustomerSegmentation:
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

    def segment_customers(self, n_clusters=3):
        feature_columns = [
            'avg_purchase_amount', 
            'purchase_frequency', 
            'avg_purchase_quantity', 
            'category_diversity'
        ]
        
        # Calculate features
        customer_features = self.df.groupBy("customer_id").agg(
            F.avg("total_amount").alias("avg_purchase_amount"),
            F.count("transaction_id").alias("purchase_frequency"),
            F.avg("quantity").alias("avg_purchase_quantity"),
            F.countDistinct("category").alias("category_diversity")
        )

        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

        # Feature scaling
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

        # K-means model
        kmeans = KMeans(k=n_clusters, featuresCol="scaled_features", predictionCol="cluster")

        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        model = pipeline.fit(customer_features)
        clustered_data = model.transform(customer_features)

        # Cluster summary
        cluster_summary = clustered_data.groupBy("cluster").agg(
            F.count("*").alias("customer_count"),
            F.avg("avg_purchase_amount").alias("avg_purchase_amount"),
            F.avg("purchase_frequency").alias("purchase_frequency"),
            F.avg("avg_purchase_quantity").alias("avg_purchase_quantity"),
            F.avg("category_diversity").alias("category_diversity")
        )
        
        print("Customer Segment Summary:")
        cluster_summary.show()

        return {
            "model": model,
            "clustered_customers": clustered_data,
            "cluster_summary": cluster_summary
        }