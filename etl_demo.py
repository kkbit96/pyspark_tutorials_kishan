"""
Demo script showing how to use the SnowflakeETLProcessor
This demonstrates the complete ETL workflow with sample data
"""

from etl_snowflake_integration import SnowflakeETLProcessor, AWSServiceIntegration
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json

def create_sample_data():
    """Create sample data for demonstration"""
    
    # Sample employee data
    employee_data = [
        ("EMP001", "john doe", "john.doe@company.com", "IT", 75000, "2024-01-15"),
        ("EMP002", "JANE SMITH", "jane.smith@company.com", "HR", 65000, "2024-01-16"),
        ("EMP003", "bob johnson", "bob.johnson@company.com", "Finance", 80000, "2024-01-17"),
        ("EMP004", "alice brown", "alice.brown@company.com", "IT", 70000, "2024-01-18"),
        ("EMP005", "charlie wilson", "charlie.wilson@company.com", "Marketing", 60000, "2024-01-19")
    ]
    
    # Sample customer data
    customer_data = [
        ("CUST001", "mike davis", "mike.davis@email.com", "Premium", "2024-01-10"),
        ("CUST002", "SARAH MILLER", "sarah.miller@email.com", "Standard", "2024-01-11"),
        ("CUST003", "david garcia", "david.garcia@email.com", "Premium", "2024-01-12"),
        ("CUST004", "lisa anderson", "lisa.anderson@email.com", "Basic", "2024-01-13"),
        ("CUST005", "tom taylor", "tom.taylor@email.com", "Standard", "2024-01-14")
    ]
    
    return employee_data, customer_data

def demo_etl_process():
    """Demonstrate the ETL process with sample data"""
    
    print("=== Snowflake ETL Process Demonstration ===\n")
    
    # Initialize ETL processor
    print("1. Initializing ETL processor...")
    etl_processor = SnowflakeETLProcessor(app_name="ETLDemo")
    
    try:
        # Create sample DataFrames
        print("2. Creating sample data...")
        employee_data, customer_data = create_sample_data()
        
        # Employee schema
        employee_schema = StructType([
            StructField("employee_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("hire_date", StringType(), True)
        ])
        
        # Customer schema
        customer_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("registration_date", StringType(), True)
        ])
        
        # Create DataFrames
        employee_df = etl_processor.spark.createDataFrame(employee_data, employee_schema)
        customer_df = etl_processor.spark.createDataFrame(customer_data, customer_schema)
        
        print(f"   - Employee records: {employee_df.count()}")
        print(f"   - Customer records: {customer_df.count()}")
        
        # Demonstrate transformations
        print("\n3. Demonstrating data transformations...")
        
        # Transform employee data
        employee_config = {
            "standardize_names": True,
            "validate_emails": True,
            "calculate_metrics": True,
            "add_metadata": True
        }
        
        transformed_employee_df = etl_processor.transform_data(employee_df, employee_config)
        print("   - Employee data transformed successfully")
        
        # Show transformation results
        print("\n   Transformed Employee Data Sample:")
        transformed_employee_df.select("employee_id", "name", "email_valid", "salary_band", "processed_date").show(5, truncate=False)
        
        # Transform customer data
        customer_config = {
            "standardize_names": True,
            "validate_emails": True,
            "add_metadata": True
        }
        
        transformed_customer_df = etl_processor.transform_data(customer_df, customer_config)
        print("   - Customer data transformed successfully")
        
        # Demonstrate XML conversion
        print("\n4. Demonstrating XML conversion...")
        
        xml_schema = {
            "root_element": "employees",
            "record_element": "employee",
            "profile_attributes": {
                "version": "1.0",
                "source": "HR_SYSTEM",
                "generated_by": "ETL_PROCESS"
            },
            "exclude_fields": ["record_id"]
        }
        
        xml_output = etl_processor.convert_to_xml(transformed_employee_df.limit(3), xml_schema)
        print("   - XML conversion completed")
        print("\n   Generated XML Sample:")
        print(xml_output[:500] + "..." if len(xml_output) > 500 else xml_output)
        
        # Demonstrate data quality checks
        print("\n5. Demonstrating data quality checks...")
        
        # Check for null values
        null_counts = transformed_employee_df.select([
            sum(col("employee_id").isNull().cast("int")).alias("null_employee_ids"),
            sum(col("name").isNull().cast("int")).alias("null_names"),
            sum(col("email").isNull().cast("int")).alias("null_emails")
        ]).collect()[0]
        
        print(f"   - Null employee IDs: {null_counts['null_employee_ids']}")
        print(f"   - Null names: {null_counts['null_names']}")
        print(f"   - Null emails: {null_counts['null_emails']}")
        
        # Check email validation
        email_validation = transformed_employee_df.groupBy("email_valid").count().collect()
        print("   - Email validation results:")
        for row in email_validation:
            print(f"     Valid emails: {row['count']}" if row['email_valid'] else f"     Invalid emails: {row['count']}")
        
        # Demonstrate aggregation
        print("\n6. Demonstrating data aggregation...")
        
        # Department-wise salary summary
        dept_summary = transformed_employee_df.groupBy("department") \
            .agg(
                count("*").alias("employee_count"),
                avg("salary").alias("avg_salary"),
                min("salary").alias("min_salary"),
                max("salary").alias("max_salary")
            ) \
            .orderBy("department")
        
        print("   - Department-wise salary summary:")
        dept_summary.show()
        
        # Salary band distribution
        salary_band_dist = transformed_employee_df.groupBy("salary_band") \
            .count() \
            .withColumn("percentage", col("count") * 100 / transformed_employee_df.count()) \
            .orderBy("count", ascending=False)
        
        print("   - Salary band distribution:")
        salary_band_dist.show()
        
        print("\n7. ETL Process Summary:")
        print("   ✓ Data extraction from multiple sources")
        print("   ✓ Data transformation with profile-specific rules")
        print("   ✓ Data quality validation")
        print("   ✓ XML format conversion")
        print("   ✓ Data aggregation and reporting")
        print("   ✓ Ready for Snowflake loading")
        
        print("\n=== ETL Demonstration Completed Successfully ===")
        
    except Exception as e:
        print(f"Error during ETL demonstration: {str(e)}")
        
    finally:
        # Clean up
        etl_processor.stop()
        print("\nSpark session stopped.")

def demo_aws_integration():
    """Demonstrate AWS services integration"""
    
    print("\n=== AWS Services Integration Demonstration ===\n")
    
    try:
        # Initialize AWS integration
        aws_integration = AWSServiceIntegration()
        
        print("AWS Services Integration Features:")
        print("1. AWS Glue ETL Jobs")
        print("   - Serverless ETL processing")
        print("   - Automatic scaling")
        print("   - Built-in data catalog integration")
        
        print("\n2. AWS Lambda Functions")
        print("   - Event-driven processing")
        print("   - Microservices architecture")
        print("   - Cost-effective for small workloads")
        
        print("\n3. AWS Step Functions")
        print("   - Workflow orchestration")
        print("   - Error handling and retries")
        print("   - Visual workflow management")
        
        print("\n4. AWS S3 Integration")
        print("   - Scalable data storage")
        print("   - Multiple data formats support")
        print("   - Versioning and lifecycle policies")
        
        print("\nExample Step Function Workflow:")
        print("Extract → Transform → Load → Validate → Archive")
        
    except Exception as e:
        print(f"Error in AWS integration demo: {str(e)}")

def demo_snowflake_procedures():
    """Demonstrate Snowflake stored procedures"""
    
    print("\n=== Snowflake Stored Procedures Demonstration ===\n")
    
    procedures_info = {
        "VALIDATE_DATA_QUALITY": {
            "description": "Validates data quality by checking for nulls and duplicates",
            "parameters": ["TABLE_NAME"],
            "returns": "Quality metrics summary"
        },
        "CREATE_AGGREGATED_REPORTS": {
            "description": "Creates department-wise and salary band reports",
            "parameters": [],
            "returns": "Success confirmation"
        },
        "ARCHIVE_OLD_DATA": {
            "description": "Archives old data based on retention policy",
            "parameters": ["DAYS_TO_RETAIN"],
            "returns": "Archive summary"
        }
    }
    
    print("Available Stored Procedures:")
    for proc_name, info in procedures_info.items():
        print(f"\n{proc_name}:")
        print(f"  Description: {info['description']}")
        print(f"  Parameters: {', '.join(info['parameters']) if info['parameters'] else 'None'}")
        print(f"  Returns: {info['returns']}")
    
    print("\nExample Usage:")
    print("CALL VALIDATE_DATA_QUALITY('EMPLOYEE_DATA');")
    print("CALL CREATE_AGGREGATED_REPORTS();")
    print("CALL ARCHIVE_OLD_DATA(365);")

if __name__ == "__main__":
    # Run demonstrations
    demo_etl_process()
    demo_aws_integration()
    demo_snowflake_procedures()
    
    print("\n" + "="*60)
    print("All demonstrations completed!")
    print("="*60)

