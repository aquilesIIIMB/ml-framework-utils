from typing import Dict, Optional, Sequence, Tuple
import argparse

from google.cloud import aiplatform
from google.cloud.aiplatform import explain


def upload_model_sample(
    project: str,
    location: str,
    display_name: str,
    serving_container_image_uri: str,
    parent_model: str = None,
    artifact_uri: Optional[str] = None,
    serving_container_predict_route: Optional[str] = None,
    serving_container_health_route: Optional[str] = None,
    description: Optional[str] = None,
    serving_container_command: Optional[Sequence[str]] = None,
    serving_container_args: Optional[Sequence[str]] = None,
    serving_container_environment_variables: Optional[Dict[str, str]] = None,
    serving_container_ports: Optional[Sequence[int]] = None,
    instance_schema_uri: Optional[str] = None,
    parameters_schema_uri: Optional[str] = None,
    prediction_schema_uri: Optional[str] = None,
    explanation_metadata: Optional[explain.ExplanationMetadata] = None,
    explanation_parameters: Optional[explain.ExplanationParameters] = None,
    sync: bool = True,
    model_id: Optional[str] = None,
    staging_bucket: Optional[str] = None,
):

    aiplatform.init(project=project, location=location, staging_bucket=staging_bucket)

    model = aiplatform.Model.upload(
        display_name=display_name,
        artifact_uri=artifact_uri,
        serving_container_image_uri=serving_container_image_uri,
        serving_container_predict_route=serving_container_predict_route,
        serving_container_health_route=serving_container_health_route,
        instance_schema_uri=instance_schema_uri,
        parameters_schema_uri=parameters_schema_uri,
        prediction_schema_uri=prediction_schema_uri,
        description=description,
        serving_container_command=serving_container_command,
        serving_container_args=serving_container_args,
        serving_container_environment_variables=serving_container_environment_variables,
        serving_container_ports=serving_container_ports,
        explanation_metadata=explanation_metadata,
        explanation_parameters=explanation_parameters,
        sync=sync,
        is_default_version=True,
        model_id=model_id,
        parent_model=parent_model,
        labels={"applicationName": "{{cookiecutter.applicationName}}", "gitProject": "{{cookiecutter.projectName}}"}
    )

    model.wait()

    print(model.display_name)
    print(model.resource_name)
    return model


def create_endpoint_sample(
    project: str,
    display_name: str,
    location: str,
):
    aiplatform.init(project=project, location=location, staging_bucket=staging_bucket)

    endpoint = aiplatform.Endpoint.create(
        display_name=display_name,
        project=project,
        location=location,
        labels={"applicationName": "{{cookiecutter.applicationName}}", "gitProject": "{{cookiecutter.projectName}}"}
    )

    print(endpoint.display_name)
    print(endpoint.resource_name)
    return endpoint


def deploy_model_with_dedicated_resources_sample(
    project,
    location,
    model: aiplatform.Model,
    machine_type: str,
    service_account: Optional[str] = None,
    endpoint: Optional[aiplatform.Endpoint] = None,
    deployed_model_display_name: Optional[str] = None,
    traffic_percentage: Optional[int] = 0,
    traffic_split: Optional[Dict[str, int]] = None,
    min_replica_count: int = 1,
    max_replica_count: int = 1,
    accelerator_type: Optional[str] = None,
    accelerator_count: Optional[int] = None,
    explanation_metadata: Optional[explain.ExplanationMetadata] = None,
    explanation_parameters: Optional[explain.ExplanationParameters] = None,
    metadata: Optional[Sequence[Tuple[str, str]]] = (),
    sync: bool = True,
):
    """
    model_name: A fully-qualified model resource name or model ID.
          Example: "projects/123/locations/us-central1/models/456" or
          "456" when project and location are initialized or passed.
    """

    aiplatform.init(project=project, location=location, staging_bucket=staging_bucket)

    # The explanation_metadata and explanation_parameters should only be
    # provided for a custom trained model and not an AutoML model.
    model.deploy(
        endpoint=endpoint,
        traffic_percentage=traffic_percentage,
        traffic_split=traffic_split,
        machine_type=machine_type,
        min_replica_count=min_replica_count,
        max_replica_count=max_replica_count,
        accelerator_type=accelerator_type,
        accelerator_count=accelerator_count,
        explanation_metadata=explanation_metadata,
        explanation_parameters=explanation_parameters,
        metadata=metadata,
        sync=sync,
        service_account=service_account,
    )

    model.wait()

    print(model.display_name)
    print(model.resource_name)
    return model


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--service_account',
        help='Service account to deploy the service in Vertex Online Prediction.',
        type=str,
        required=True
    )
    parser.add_argument(
        '--version',
        help='Instance version.',
        type=str,
        required=True
    )
    parser.add_argument(
        "--bucket_name",
        help="Bucket to stage local model artifacts. Overrides staging_bucket set in aiplatform.init.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--model_name", 
        help="Model name to be deployed.", 
        type=str, 
        default=None
    )
    parser.add_argument(
        "--project_id", 
        help="Project to upload this model to. Overrides project set in aiplatform.init.", 
        type=str, 
        default=None
    )
    parser.add_argument(
        '--location',
        help='Location to upload this model to. Overrides location set in aiplatform.init.',
        type=str,
        required=True
    )
    parser.add_argument(
        "--serving_container_image_uri",
        help="The URI of the Model serving container.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--endpoint_name", 
        help="Endpoint name to be deployed in Vertex Online Prediction.", 
        type=str, 
        default=None
    )
    parser.add_argument(
        '--cpu_machine_name',
        help='The type of machine.',
        type=str,
        required=True
    )
    parser.add_argument(
        '--gpu_machine_name',
        help='Hardware accelerator type. Must also set accelerator_count if used. One of ACCELERATOR_TYPE_UNSPECIFIED, NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4, NVIDIA_TESLA_T4.',
        type=str,
        default=None
    )
    parser.add_argument(
        '--gpu_machine_cores',
        help='The number of accelerators to attach to a worker replica.',
        type=int,
        default=None
    )
    parser.add_argument(
        "--container_port",
        help="Port that Vertex Online Prediction enbale to be predictions.",
        type=int,
        default=8000
    )
    parser.add_argument(
        "--app_port", 
        help="Port that App use to be deployed.", 
        type=int, 
        default=8080
    )
    
    args = parser.parse_args()
    
    # gpu_cores and gpu_type must have value both or neither
    if args.gpu_machine_name or args.gpu_machine_cores:
        if args.gpu_machine_name is None or args.gpu_machine_cores is None:
            raise ValueError('If you need GPU you have to set a value for "gpu_cores" and "gpu_type"')
    
    serving_container_ports=[args.container_port]
    serving_container_args=[f'--app_port={args.app_port}']

    staging_bucket=f'gs://{args.bucket_name}'
    artifact_uri = f'gs://{args.bucket_name}/models/{args.model_name}/metadata/{args.version}/'
    parent_model = f'projects/{args.project_id}/locations/{args.location}/models/{args.model_name}'
    
    if aiplatform.Model.list(filter=f'display_name={args.model_name}'):
        # If model_id exists, It's created a model version
        model = upload_model_sample(
            project=args.project_id,
            location=args.location,
            display_name=args.model_name,
            serving_container_image_uri=args.serving_container_image_uri,
            artifact_uri=artifact_uri,
            model_id=args.model_name,
            staging_bucket=staging_bucket,
            parent_model=parent_model,
            serving_container_ports=serving_container_ports,
            serving_container_args=serving_container_args,
        )
    else:
        model = upload_model_sample(
            project=args.project_id,
            location=args.location,
            display_name=args.model_name,
            serving_container_image_uri=args.serving_container_image_uri,
            artifact_uri=artifact_uri,
            model_id=args.model_name,
            staging_bucket=staging_bucket,
            serving_container_ports=serving_container_ports,
            serving_container_args=serving_container_args,
        )

    endpoint = create_endpoint_sample(
        project=args.project_id,
        display_name=args.endpoint_name,
        location=args.location,
    )

    deployed_model = deploy_model_with_dedicated_resources_sample(
        project=args.project_id,
        location=args.location,
        service_account=args.service_account,
        machine_type=args.cpu_machine_name,
        accelerator_type=args.gpu_machine_name,
        accelerator_count=args.gpu_machine_cores,
        model=model,
        endpoint=endpoint
    )
