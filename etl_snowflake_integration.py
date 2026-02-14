"""
ETL Process Implementation for Snowflake Data Warehouse Integration
Demonstrates PySpark ETL processes, AWS S3 integration, and Snowflake data warehousing

This script implements:
1. ETL processes to extract, transform and load data from various sources into Snowflake
2. Conversion of raw data from AWS S3 into standardized XML formats across multiple profiles
3. AWS Glue ETL tasks and AWS services (S3, Lambda, Step Functions) orchestration
4. PySpark and Snowflake data warehousing solutions with stored procedures
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeETLProcessor:
    """
    Main ETL processor class for Snowflake data warehouse integration
    """
    
    def __init__(self, app_name: str = "SnowflakeETL"):
        """Initialize Spark session with Snowflake connector configuration"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Snowflake connection parameters (these would typically come from environment variables or config)
        self.snowflake_options = {
            "sfURL": "your_account.snowflakecomputing.com",
            "sfUser": "your_username",
            "sfPassword": "your_password",
            "sfDatabase": "your_database",
            "sfSchema": "your_schema",
            "sfWarehouse": "your_warehouse",
            "sfRole": "your_role"
        }
        
        # AWS configuration
        self.aws_region = "us-east-1"
        self.s3_bucket = "your-etl-bucket"
        
    def extract_from_s3(self, s3_path: str, file_format: str = "json") -> DataFrame:
        """
        Extract data from AWS S3 with support for multiple file formats
        
        Args:
            s3_path: S3 path to the data files
            file_format: Format of the files (json, csv, parquet, xml)
            
        Returns:
            PySpark DataFrame
        """
        logger.info(f"Extracting data from S3: {s3_path}")
        
        try:
            if file_format.lower() == "json":
                df = self.spark.read.option("multiline", "true").json(f"s3a://{self.s3_bucket}/{s3_path}")
            elif file_format.lower() == "csv":
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(f"s3a://{self.s3_bucket}/{s3_path}")
            elif file_format.lower() == "parquet":
                df = self.spark.read.parquet(f"s3a://{self.s3_bucket}/{s3_path}")
            elif file_format.lower() == "xml":
                # For XML files, we'll use a custom approach
                df = self._read_xml_files(s3_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
            logger.info(f"Successfully extracted {df.count()} records from S3")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from S3: {str(e)}")
            raise
    
    def _read_xml_files(self, s3_path: str) -> DataFrame:
        """Custom method to read XML files from S3"""
        # This would typically use a custom XML reader or convert to JSON first
        # For demonstration, we'll create sample data
        sample_data = [
            ("1", "John Doe", "john.doe@email.com", "IT", 75000),
            ("2", "Jane Smith", "jane.smith@email.com", "HR", 65000),
            ("3", "Bob Johnson", "bob.johnson@email.com", "Finance", 80000)
        ]
        
        schema = StructType([
            StructField("employee_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True)
        ])
        
        return self.spark.createDataFrame(sample_data, schema)
    
    def transform_data(self, df: DataFrame, profile_config: Dict) -> DataFrame:
        """
        Transform raw data according to profile-specific requirements
        
        Args:
            df: Input DataFrame
            profile_config: Configuration for the specific profile
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        try:
            # Apply profile-specific transformations
            if profile_config.get("standardize_names"):
                df = self._standardize_names(df)
            
            if profile_config.get("validate_emails"):
                df = self._validate_emails(df)
            
            if profile_config.get("calculate_metrics"):
                df = self._calculate_metrics(df)
            
            if profile_config.get("add_metadata"):
                df = self._add_metadata(df)
            
            # Apply common transformations
            df = df.withColumn("processed_date", current_timestamp()) \
                   .withColumn("record_id", monotonically_increasing_id())
            
            logger.info(f"Transformation completed. Final record count: {df.count()}")
            return df
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
    
    def _standardize_names(self, df: DataFrame) -> DataFrame:
        """Standardize name formatting"""
        return df.withColumn("name", 
                           regexp_replace(initcap(trim(col("name"))), "\\s+", " "))
    
    def _validate_emails(self, df: DataFrame) -> DataFrame:
        """Validate email addresses"""
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return df.withColumn("email_valid", 
                           col("email").rlike(email_regex))
    
    def _calculate_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate additional metrics"""
        return df.withColumn("salary_band", 
                           when(col("salary") < 50000, "Low")
                           .when(col("salary") < 80000, "Medium")
                           .otherwise("High"))
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add metadata columns"""
        return df.withColumn("etl_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
                 .withColumn("data_source", lit("S3"))
    
    def convert_to_xml(self, df: DataFrame, xml_schema: Dict) -> str:
        """
        Convert DataFrame to standardized XML format
        
        Args:
            df: Input DataFrame
            xml_schema: XML schema configuration
            
        Returns:
            XML string
        """
        logger.info("Converting DataFrame to XML format")
        
        try:
            # Collect data to driver (for small datasets)
            rows = df.collect()
            
            # Create root element
            root = ET.Element(xml_schema.get("root_element", "data"))
            root.set("timestamp", datetime.now().isoformat())
            root.set("record_count", str(len(rows)))
            
            # Add profile-specific attributes
            if "profile_attributes" in xml_schema:
                for attr, value in xml_schema["profile_attributes"].items():
                    root.set(attr, value)
            
            # Convert each row to XML
            for row in rows:
                record = ET.SubElement(root, xml_schema.get("record_element", "record"))
                
                for field in df.columns:
                    if field in xml_schema.get("exclude_fields", []):
                        continue
                    
                    element = ET.SubElement(record, field)
                    element.text = str(row[field]) if row[field] is not None else ""
            
            # Convert to string
            xml_string = ET.tostring(root, encoding='unicode')
            
            logger.info("XML conversion completed successfully")
            return xml_string
            
        except Exception as e:
            logger.error(f"Error converting to XML: {str(e)}")
            raise
    
    def load_to_snowflake(self, df: DataFrame, table_name: str, write_mode: str = "overwrite") -> None:
        """
        Load transformed data to Snowflake data warehouse
        
        Args:
            df: DataFrame to load
            table_name: Target table name in Snowflake
            write_mode: Write mode (overwrite, append, ignore, error)
        """
        logger.info(f"Loading data to Snowflake table: {table_name}")
        
        try:
            # Write to Snowflake
            df.write \
                .format("snowflake") \
                .options(**self.snowflake_options) \
                .option("dbtable", table_name) \
                .mode(write_mode) \
                .save()
            
            logger.info(f"Successfully loaded {df.count()} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading to Snowflake: {str(e)}")
            raise
    
    def execute_stored_procedure(self, procedure_name: str, parameters: Dict = None) -> None:
        """
        Execute Snowflake stored procedure
        
        Args:
            procedure_name: Name of the stored procedure
            parameters: Parameters to pass to the procedure
        """
        logger.info(f"Executing Snowflake stored procedure: {procedure_name}")
        
        try:
            # Create connection string
            conn_string = f"jdbc:snowflake://{self.snowflake_options['sfURL']}" \
                         f"/?user={self.snowflake_options['sfUser']}" \
                         f"&password={self.snowflake_options['sfPassword']}" \
                         f"&db={self.snowflake_options['sfDatabase']}" \
                         f"&schema={self.snowflake_options['sfSchema']}" \
                         f"&warehouse={self.snowflake_options['sfWarehouse']}" \
                         f"&role={self.snowflake_options['sfRole']}"
            
            # Build procedure call
            if parameters:
                param_str = ", ".join([f"{k} => {v}" for k, v in parameters.items()])
                call_statement = f"CALL {procedure_name}({param_str})"
            else:
                call_statement = f"CALL {procedure_name}()"
            
            # Execute using SQL
            self.spark.sql(call_statement)
            
            logger.info(f"Stored procedure {procedure_name} executed successfully")
            
        except Exception as e:
            logger.error(f"Error executing stored procedure: {str(e)}")
            raise
    
    def orchestrate_etl_pipeline(self, config: Dict) -> None:
        """
        Orchestrate the complete ETL pipeline
        
        Args:
            config: Pipeline configuration
        """
        logger.info("Starting ETL pipeline orchestration")
        
        try:
            for profile in config.get("profiles", []):
                logger.info(f"Processing profile: {profile['name']}")
                
                # Extract data from S3
                df = self.extract_from_s3(
                    s3_path=profile["s3_path"],
                    file_format=profile.get("file_format", "json")
                )
                
                # Transform data
                df_transformed = self.transform_data(df, profile.get("transform_config", {}))
                
                # Convert to XML if required
                if profile.get("convert_to_xml", False):
                    xml_output = self.convert_to_xml(df_transformed, profile.get("xml_schema", {}))
                    # Save XML to S3 or local storage
                    self._save_xml_output(xml_output, profile["name"])
                
                # Load to Snowflake
                self.load_to_snowflake(
                    df_transformed,
                    table_name=profile["target_table"],
                    write_mode=profile.get("write_mode", "overwrite")
                )
                
                # Execute post-load stored procedures
                if "post_load_procedures" in profile:
                    for proc in profile["post_load_procedures"]:
                        self.execute_stored_procedure(proc["name"], proc.get("parameters"))
                
                logger.info(f"Profile {profile['name']} processed successfully")
            
            logger.info("ETL pipeline orchestration completed")
            
        except Exception as e:
            logger.error(f"Error in ETL pipeline orchestration: {str(e)}")
            raise
    
    def _save_xml_output(self, xml_content: str, profile_name: str) -> None:
        """Save XML output to S3"""
        try:
            s3_client = boto3.client('s3', region_name=self.aws_region)
            key = f"xml_outputs/{profile_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
            
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=xml_content,
                ContentType='application/xml'
            )
            
            logger.info(f"XML output saved to S3: s3://{self.s3_bucket}/{key}")
            
        except ClientError as e:
            logger.error(f"Error saving XML to S3: {str(e)}")
            raise
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

# SQL Stored Procedures for Snowflake (to be created in Snowflake)
SNOWFLAKE_SQL_PROCEDURES = """
-- Create database and schema if not exists
CREATE DATABASE IF NOT EXISTS ETL_DATABASE;
CREATE SCHEMA IF NOT EXISTS ETL_DATABASE.ETL_SCHEMA;

-- Stored procedure for data quality validation
CREATE OR REPLACE PROCEDURE ETL_DATABASE.ETL_SCHEMA.VALIDATE_DATA_QUALITY(TABLE_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    total_records INTEGER;
    null_count INTEGER;
    duplicate_count INTEGER;
    result STRING;
BEGIN
    -- Count total records
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || TABLE_NAME INTO total_records;
    
    -- Count null values in critical fields
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || TABLE_NAME || ' WHERE employee_id IS NULL OR name IS NULL' INTO null_count;
    
    -- Count duplicates
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM (SELECT employee_id, COUNT(*) as cnt FROM ' || TABLE_NAME || ' GROUP BY employee_id HAVING cnt > 1)' INTO duplicate_count;
    
    -- Generate result
    result := 'Total Records: ' || total_records || ', Null Values: ' || null_count || ', Duplicates: ' || duplicate_count;
    
    RETURN result;
END;
$$;

-- Stored procedure for data aggregation and reporting
CREATE OR REPLACE PROCEDURE ETL_DATABASE.ETL_SCHEMA.CREATE_AGGREGATED_REPORTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
BEGIN
    -- Create department-wise salary summary
    CREATE OR REPLACE TABLE ETL_DATABASE.ETL_SCHEMA.DEPT_SALARY_SUMMARY AS
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        CURRENT_TIMESTAMP() as created_at
    FROM ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA
    GROUP BY department;
    
    -- Create salary band distribution
    CREATE OR REPLACE TABLE ETL_DATABASE.ETL_SCHEMA.SALARY_BAND_DISTRIBUTION AS
    SELECT 
        salary_band,
        COUNT(*) as employee_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA
    GROUP BY salary_band;
    
    result := 'Aggregated reports created successfully';
    RETURN result;
END;
$$;

-- Stored procedure for data archiving
CREATE OR REPLACE PROCEDURE ETL_DATABASE.ETL_SCHEMA.ARCHIVE_OLD_DATA(DAYS_TO_RETAIN INTEGER)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    cutoff_date DATE;
    archived_count INTEGER;
    result STRING;
BEGIN
    -- Calculate cutoff date
    cutoff_date := CURRENT_DATE() - DAYS_TO_RETAIN;
    
    -- Archive old records
    CREATE OR REPLACE TABLE ETL_DATABASE.ETL_SCHEMA.ARCHIVED_EMPLOYEE_DATA AS
    SELECT * FROM ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA
    WHERE processed_date < cutoff_date;
    
    -- Delete old records from main table
    DELETE FROM ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA
    WHERE processed_date < cutoff_date;
    
    -- Get count of archived records
    SELECT COUNT(*) INTO archived_count FROM ETL_DATABASE.ETL_SCHEMA.ARCHIVED_EMPLOYEE_DATA;
    
    result := 'Archived ' || archived_count || ' records older than ' || cutoff_date;
    RETURN result;
END;
$$;
"""

# AWS Glue Job Script Template
AWS_GLUE_JOB_SCRIPT = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from S3
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://your-bucket/input-data/"]},
    format="json"
)

# Apply transformations
transformed_data = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ("employee_id", "string", "employee_id", "string"),
        ("name", "string", "full_name", "string"),
        ("email", "string", "email_address", "string"),
        ("department", "string", "dept", "string"),
        ("salary", "int", "annual_salary", "int")
    ]
)

# Write to Snowflake
glueContext.write_dynamic_frame.from_options(
    frame=transformed_data,
    connection_type="custom.jdbc",
    connection_options={
        "url": "jdbc:snowflake://your_account.snowflakecomputing.com/",
        "dbtable": "ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA",
        "user": "your_username",
        "password": "your_password",
        "database": "ETL_DATABASE",
        "schema": "ETL_SCHEMA",
        "warehouse": "your_warehouse"
    }
)

job.commit()
"""

# Example usage and configuration
def main():
    """Main function demonstrating the ETL process"""
    
    # Initialize ETL processor
    etl_processor = SnowflakeETLProcessor()
    
    # Configuration for multiple profiles
    pipeline_config = {
        "profiles": [
            {
                "name": "employee_profile",
                "s3_path": "raw_data/employees/",
                "file_format": "json",
                "target_table": "ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA",
                "write_mode": "overwrite",
                "convert_to_xml": True,
                "xml_schema": {
                    "root_element": "employees",
                    "record_element": "employee",
                    "profile_attributes": {"version": "1.0", "source": "HR_SYSTEM"},
                    "exclude_fields": ["record_id"]
                },
                "transform_config": {
                    "standardize_names": True,
                    "validate_emails": True,
                    "calculate_metrics": True,
                    "add_metadata": True
                },
                "post_load_procedures": [
                    {"name": "ETL_DATABASE.ETL_SCHEMA.VALIDATE_DATA_QUALITY", 
                     "parameters": {"TABLE_NAME": "ETL_DATABASE.ETL_SCHEMA.EMPLOYEE_DATA"}},
                    {"name": "ETL_DATABASE.ETL_SCHEMA.CREATE_AGGREGATED_REPORTS"}
                ]
            },
            {
                "name": "customer_profile",
                "s3_path": "raw_data/customers/",
                "file_format": "csv",
                "target_table": "ETL_DATABASE.ETL_SCHEMA.CUSTOMER_DATA",
                "write_mode": "append",
                "convert_to_xml": False,
                "transform_config": {
                    "standardize_names": True,
                    "validate_emails": True,
                    "add_metadata": True
                }
            }
        ]
    }
    
    try:
        # Execute the ETL pipeline
        etl_processor.orchestrate_etl_pipeline(pipeline_config)
        
        print("ETL pipeline executed successfully!")
        
    except Exception as e:
        print(f"ETL pipeline failed: {str(e)}")
        
    finally:
        # Clean up
        etl_processor.stop()

if __name__ == "__main__":
    main()

# Additional utility functions for AWS services integration

class AWSServiceIntegration:
    """Integration with AWS services for orchestration"""
    
    def __init__(self, region: str = "us-east-1"):
        self.region = region
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.stepfunctions_client = boto3.client('stepfunctions', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)
    
    def trigger_lambda_function(self, function_name: str, payload: Dict) -> Dict:
        """Trigger AWS Lambda function"""
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            return json.loads(response['Payload'].read())
        except ClientError as e:
            logger.error(f"Error triggering Lambda function: {str(e)}")
            raise
    
    def start_step_function(self, state_machine_arn: str, input_data: Dict) -> str:
        """Start AWS Step Functions state machine"""
        try:
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=state_machine_arn,
                input=json.dumps(input_data)
            )
            return response['executionArn']
        except ClientError as e:
            logger.error(f"Error starting Step Function: {str(e)}")
            raise
    
    def trigger_glue_job(self, job_name: str, arguments: Dict = None) -> str:
        """Trigger AWS Glue job"""
        try:
            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments=arguments or {}
            )
            return response['JobRunId']
        except ClientError as e:
            logger.error(f"Error triggering Glue job: {str(e)}")
            raise

# Example Step Functions state machine definition (JSON)
STEP_FUNCTION_DEFINITION = {
    "Comment": "ETL Pipeline Orchestration",
    "StartAt": "ExtractData",
    "States": {
        "ExtractData": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:region:account:function:ExtractDataFunction",
            "Next": "TransformData",
            "Retry": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "IntervalSeconds": 2,
                    "MaxAttempts": 3
                }
            ]
        },
        "TransformData": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "ETL-TransformJob"
            },
            "Next": "LoadToSnowflake"
        },
        "LoadToSnowflake": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:region:account:function:LoadToSnowflakeFunction",
            "Next": "ValidateData"
        },
        "ValidateData": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:region:account:function:ValidateDataFunction",
            "End": True
        }
    }
}

