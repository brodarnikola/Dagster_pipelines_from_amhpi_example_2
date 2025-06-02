"""
Create OpenMetadata pipeline service
Run this once before tracking lineage
"""
from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
from metadata.generated.schema.entity.services.connections.pipeline.dagsterConnection import DagsterConnection
from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.ingestion.ometa.ometa_api import OpenMetadata

def create_pipeline_service():
    # Initialize OpenMetadata client
    server_config = OpenMetadataConnection(hostPort="http://openmetadata-server:8585/api")
    metadata = OpenMetadata(server_config)
    
    # Create pipeline service
    service = CreatePipelineServiceRequest(
        name="dagster_service",
        serviceType=PipelineServiceType.Dagster,
        connection=DagsterConnection(
            config=DagsterConnection.Config(
                host="http://dagster-webserver:3000"
            )
        )
    )
    
    # Add service to OpenMetadata
    result = metadata.create_or_update(service)
    print(f"Created service: {result.name}")
    return result

# if __name__ == "__main__":
#     create_pipeline_service()