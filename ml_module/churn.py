import os
import traceback
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.functions import col, when, to_timestamp, month, year, count, avg, expr
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.classification import RandomForestClassifier

class CustomerChurnMLAnalysis:
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
                F.countDistinct("category").alias("unique_categories")
            )
        
        # Calculate recency (days since last purchase)
        customer_metrics = customer_metrics.withColumn(
            "recency_days", 
            F.datediff(F.current_date(), F.to_date(col("last_purchase_date")))
        )
        
        return customer_metrics
    
    def predict_customer_churn(self, feature_columns=None):
        if feature_columns is None:
            feature_columns = [
                'total_transactions', 'avg_transaction_amount', 
                'total_purchase_amount', 'recency_days', 
                'unique_products_purchased', 'unique_categories', 
                'customer_type', 'city'
            ]
        
        # Calculate customer metrics
        customer_metrics = self._calculate_customer_metrics()

        intervals = customer_metrics.agg(
        F.min("recency_days").alias("min_recency_days"),
        F.max("recency_days").alias("max_recency_days"),
        F.min("total_transactions").alias("min_total_transactions"),
        F.max("total_transactions").alias("max_total_transactions"),
        F.min("total_purchase_amount").alias("min_total_purchase_amount"),
        F.max("total_purchase_amount").alias("max_total_purchase_amount")
        ).collect()
        
        # Churn based on multiple criteria
        churn_conditions = (
            (col("recency_days") > 60) |  # No purchase in last 3 months
            (col("total_transactions") < 50) |  # Low transaction frequency
            (col("total_purchase_amount") < customer_metrics.approxQuantile("total_purchase_amount", [0.50], 0.01)[0])
        )

        churn_df = customer_metrics.withColumn("churn", 
            when(churn_conditions, 1).otherwise(0)
        )
        
        # Prepare features using the new flexible method
        pipeline, features_col = self._prepare_features(churn_df, feature_columns)
        
        # Split data
        train_df, test_df = churn_df.randomSplit([0.7, 0.3], seed=42)
        
        # Fit pipeline
        pipeline_model = pipeline.fit(train_df)
        prepared_train = pipeline_model.transform(train_df)
        prepared_test = pipeline_model.transform(test_df)
        
        # Logistic Regression
        lr_results = self.lr_model(features_col, prepared_train, prepared_test, feature_columns)

        # Random Forest
        rf_results = self.rf_model(features_col, prepared_train, prepared_test, feature_columns)

        return {
            "logistic_regression": lr_results,
            "random_forest": rf_results
        }   
    
    def _get_feature_importance(self, model, feature_columns):
        """
        Extract feature importance from model
        """
        coefficients = model.coefficients.toArray()
        
        # Create DataFrame and Sort by importance in descending order
        schema = StructType([
            StructField("feature", StringType(), True),
            StructField("importance", DoubleType(), True)
        ])
        
        rows = [
            (feature, float(abs(coef))) 
            for feature, coef in zip(feature_columns, coefficients)
        ]
        
        feature_importance = self.spark.createDataFrame(rows, schema)
        return feature_importance.orderBy(col("importance").desc())
    
    def lr_model(self, features_col, prepared_train, prepared_test, feature_columns):
        lr = LogisticRegression(
            featuresCol=features_col, 
            labelCol="churn", 
            maxIter=20,
            regParam=0.01
        )
        model = lr.fit(prepared_train)
        
        # Predict
        predictions = model.transform(prepared_test)
        
        # Evaluation metrics
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="churn", 
            metricName="areaUnderROC"
        )
        multiclass_evaluator = MulticlassClassificationEvaluator(
            labelCol="churn", 
            metricName="f1"
        )
        
        auc = binary_evaluator.evaluate(predictions)
        f1_score = multiclass_evaluator.evaluate(predictions)
        
        churn_summary = predictions.groupBy("churn").agg(
            count("*").alias("count"),
            avg("total_transactions").alias("avg_transactions"),
            avg("total_purchase_amount").alias("avg_purchase_amount"),
            avg("recency_days").alias("avg_recency_days")
        )
        
        # Feature importance
        feature_importance = self._get_feature_importance(model, feature_columns)
        
        return {
            "model": model,
            "predictions": predictions, 
            "auc": auc, 
            "f1_score": f1_score,
            "feature_importance": feature_importance,
            "churn_summary": churn_summary,
        }
    
    def rf_model(self, features_col, prepared_train, prepared_test, feature_columns):
        rf = RandomForestClassifier(
            featuresCol=features_col, 
            labelCol="churn", 
            maxDepth=5,
            numTrees=100
        )
        model = rf.fit(prepared_train)
        
        # Predict
        predictions = model.transform(prepared_test)
        
        # Evaluation metrics
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="churn", 
            metricName="areaUnderROC"
        )
        multiclass_evaluator = MulticlassClassificationEvaluator(
            labelCol="churn", 
            metricName="f1"
        )
        
        auc = binary_evaluator.evaluate(predictions)
        f1_score = multiclass_evaluator.evaluate(predictions)

        churn_summary = predictions.groupBy("churn").agg(
            count("*").alias("count"),
            avg("total_transactions").alias("avg_transactions"),
            avg("total_purchase_amount").alias("avg_purchase_amount"),
            avg("recency_days").alias("avg_recency_days")
        )
        
        return {
            "model": model,
            "predictions": predictions, 
            "auc": auc, 
            "f1_score": f1_score,
            "churn_summary": churn_summary,
        }