"""
Separate script to track OpenMetadata lineage for Dagster pipeline
"""
from metadata.generated.schema.entity.data.pipeline import Pipeline, Task
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.ingestion.ometa.ometa_api import OpenMetadata

def track_dagster_lineage():
    # Initialize OpenMetadata client
    server_config = OpenMetadataConnection(hostPort="http://openmetadata-server:8585/api")
    metadata = OpenMetadata(server_config)
    
    # Define pipeline with tasks and lineage relationships
    pipeline = Pipeline(
        name="simple_pipeline_job",
        displayName="Simple CSV Processing Pipeline",
        description="Pipeline that processes CSV data and loads to PostgreSQL",
        service="dagster_service",  # Must match existing service in OpenMetadata
        tasks=[
            Task(name="csv_file_input", 
                 description="Reads data from CSV file", 
                 upstreamTasks=[], 
                 downstreamTasks=["filtered_data"]),
                 
            Task(name="filtered_data", 
                 description="Filters rows based on condition", 
                 upstreamTasks=["csv_file_input"], 
                 downstreamTasks=["sorted_data"]),
                 
            Task(name="sorted_data", 
                 description="Sorts the filtered data", 
                 upstreamTasks=["filtered_data"], 
                 downstreamTasks=["postgres_output"]),
                 
            Task(name="postgres_output", 
                 description="Writes data to PostgreSQL", 
                 upstreamTasks=["sorted_data"], 
                 downstreamTasks=[])
        ]
    )
    
    # Create or update pipeline in OpenMetadata
    result = metadata.create_or_update(pipeline)
    print(f"Created pipeline: {result.fullyQualifiedName}")
    return result

# if __name__ == "__main__":
#     track_dagster_lineage()