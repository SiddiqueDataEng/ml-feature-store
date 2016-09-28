"""
ML Feature Store Implementation
Centralized feature management with online/offline serving
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import redis
import json
from datetime import datetime
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureStore:
    """Feature Store for ML features"""
    
    def __init__(self, spark_session=None, redis_host='localhost', redis_port=6379):
        self.spark = spark_session or self._create_spark_session()
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.feature_registry = {}
        
    def _create_spark_session(self):
        """Create Spark session with Delta Lake"""
        return SparkSession.builder \
            .appName("Feature Store") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    def register_feature(self, feature_name: str, feature_config: Dict[str, Any]):
        """Register a new feature"""
        logger.info(f"Registering feature: {feature_name}")
        
        feature_metadata = {
            'name': feature_name,
            'description': feature_config.get('description', ''),
            'feature_type': feature_config.get('type', 'numerical'),
            'entity': feature_config.get('entity', 'user'),
            'source_table': feature_config.get('source_table'),
            'transformation': feature_config.get('transformation'),
            'version': feature_config.get('version', '1.0'),
            'created_at': datetime.now().isoformat(),
            'tags': feature_config.get('tags', [])
        }
        
        self.feature_registry[feature_name] = feature_metadata
        
        # Store in Delta Lake
        metadata_df = self.spark.createDataFrame([feature_metadata])
        metadata_df.write \
            .format("delta") \
            .mode("append") \
            .save("/feature_store/metadata")
        
        return feature_metadata
    
    def compute_features(self, feature_names: List[str], entity_df, timestamp_col=None):
        """Compute features for given entities"""
        logger.info(f"Computing features: {feature_names}")
        
        result_df = entity_df
        
        for feature_name in feature_names:
            if feature_name not in self.feature_registry:
                raise ValueError(f"Feature {feature_name} not registered")
            
            feature_config = self.feature_registry[feature_name]
            source_table = feature_config['source_table']
            
            # Read feature data
            feature_df = self.spark.read.format("delta").load(source_table)
            
            # Point-in-time join if timestamp provided
            if timestamp_col:
                feature_df = feature_df.filter(
                    col("timestamp") <= col(timestamp_col)
                )
            
            # Join with entity
            result_df = result_df.join(
                feature_df,
                on=feature_config['entity'],
                how='left'
            )
        
        return result_df
    
    def write_features_offline(self, features_df, feature_group_name: str):
        """Write features to offline store (Delta Lake)"""
        logger.info(f"Writing features to offline store: {feature_group_name}")
        
        path = f"/feature_store/offline/{feature_group_name}"
        
        features_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .write \
            .format("delta") \
            .mode("append") \
            .partitionBy("date") \
            .save(path)
        
        logger.info(f"Features written to {path}")
    
    def write_features_online(self, features_df, entity_col: str, feature_cols: List[str]):
        """Write features to online store (Redis)"""
        logger.info(f"Writing features to online store")
        
        # Convert to Pandas for Redis insertion
        features_pd = features_df.select(entity_col, *feature_cols).toPandas()
        
        pipe = self.redis_client.pipeline()
        
        for _, row in features_pd.iterrows():
            entity_id = row[entity_col]
            features = {col: row[col] for col in feature_cols}
            
            # Store as JSON in Redis
            key = f"features:{entity_id}"
            pipe.set(key, json.dumps(features))
            pipe.expire(key, 86400)  # 24 hour TTL
        
        pipe.execute()
        logger.info(f"Wrote {len(features_pd)} feature sets to Redis")
    
    def get_online_features(self, entity_ids: List[str]) -> Dict[str, Dict]:
        """Get features from online store"""
        logger.info(f"Fetching online features for {len(entity_ids)} entities")
        
        pipe = self.redis_client.pipeline()
        for entity_id in entity_ids:
            pipe.get(f"features:{entity_id}")
        
        results = pipe.execute()
        
        features = {}
        for entity_id, result in zip(entity_ids, results):
            if result:
                features[entity_id] = json.loads(result)
            else:
                features[entity_id] = None
        
        return features
    
    def get_offline_features(self, feature_group_name: str, start_date=None, end_date=None):
        """Get features from offline store"""
        logger.info(f"Fetching offline features: {feature_group_name}")
        
        path = f"/feature_store/offline/{feature_group_name}"
        df = self.spark.read.format("delta").load(path)
        
        if start_date:
            df = df.filter(col("date") >= start_date)
        if end_date:
            df = df.filter(col("date") <= end_date)
        
        return df
    
    def get_feature_statistics(self, feature_name: str):
        """Get statistics for a feature"""
        if feature_name not in self.feature_registry:
            raise ValueError(f"Feature {feature_name} not registered")
        
        feature_config = self.feature_registry[feature_name]
        source_table = feature_config['source_table']
        
        df = self.spark.read.format("delta").load(source_table)
        
        stats = df.select(
            count(feature_name).alias("count"),
            mean(feature_name).alias("mean"),
            stddev(feature_name).alias("stddev"),
            min(feature_name).alias("min"),
            max(feature_name).alias("max")
        ).collect()[0]
        
        return {
            'feature_name': feature_name,
            'count': stats['count'],
            'mean': stats['mean'],
            'stddev': stats['stddev'],
            'min': stats['min'],
            'max': stats['max']
        }
    
    def monitor_feature_drift(self, feature_name: str, reference_date, current_date):
        """Monitor feature drift between two time periods"""
        logger.info(f"Monitoring drift for feature: {feature_name}")
        
        feature_config = self.feature_registry[feature_name]
        source_table = feature_config['source_table']
        
        df = self.spark.read.format("delta").load(source_table)
        
        # Reference distribution
        ref_stats = df.filter(col("date") == reference_date) \
            .select(mean(feature_name).alias("mean"), stddev(feature_name).alias("std")) \
            .collect()[0]
        
        # Current distribution
        curr_stats = df.filter(col("date") == current_date) \
            .select(mean(feature_name).alias("mean"), stddev(feature_name).alias("std")) \
            .collect()[0]
        
        # Calculate drift
        mean_drift = abs(curr_stats['mean'] - ref_stats['mean']) / ref_stats['mean']
        std_drift = abs(curr_stats['std'] - ref_stats['std']) / ref_stats['std']
        
        return {
            'feature_name': feature_name,
            'reference_date': reference_date,
            'current_date': current_date,
            'mean_drift': mean_drift,
            'std_drift': std_drift,
            'drift_detected': mean_drift > 0.1 or std_drift > 0.1
        }


# Example usage
def example_usage():
    """Example feature store usage"""
    
    fs = FeatureStore()
    
    # Register features
    fs.register_feature('user_total_purchases', {
        'description': 'Total number of purchases by user',
        'type': 'numerical',
        'entity': 'user_id',
        'source_table': '/data/user_purchases',
        'version': '1.0',
        'tags': ['user', 'behavioral']
    })
    
    # Compute features
    entity_df = fs.spark.createDataFrame([
        (1, '2024-01-01'),
        (2, '2024-01-01')
    ], ['user_id', 'date'])
    
    features_df = fs.compute_features(
        ['user_total_purchases'],
        entity_df,
        timestamp_col='date'
    )
    
    # Write to offline store
    fs.write_features_offline(features_df, 'user_features')
    
    # Write to online store
    fs.write_features_online(features_df, 'user_id', ['user_total_purchases'])
    
    # Get online features
    online_features = fs.get_online_features(['1', '2'])
    print(online_features)


if __name__ == "__main__":
    example_usage()
