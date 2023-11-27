import warnings
import argparse


def find_suitable_gcp_machine(
    selected_cpu_cores: int, 
    selected_ram_gb: int, 
    selected_gpu_cores: int=None, 
    selected_gpu_type: str=None
):
    """
    This function selects an appropriate Google Cloud Platform (GCP) machine type based on specified CPU cores,
    RAM, and optionally, GPU requirements. It checks against a predefined list of GCP machine types, each with
    its own CPU, RAM, and supported GPU configurations. The function ensures compatibility and returns the most
    suitable machine configuration. It performs validation for GPU requirements and raises warnings if the
    exact requested resources are not available, suggesting the closest alternatives.

    Parameters:
    - selected_cpu_cores (int): Number of CPU cores required.
    - selected_ram_gb (int): Amount of RAM required in gigabytes.
    - selected_gpu_cores (int, optional): Number of GPU cores required, to be provided with selected_gpu_type.
    - selected_gpu_type (str, optional): Type of GPU required, to be provided with selected_gpu_cores.

    Returns:
    - A dictionary containing the closest matching machine type for the requested CPU and RAM specifications.
      If GPU requirements are specified, includes the closest matching GPU machine type and cores. Issues
      warnings for any adjustments made to the requested resources.

    Raises:
    - ValueError: If only one of gpu_cores or gpu_type is provided without the other.
    - ValueError: If the specified gpu_type is not compatible with the selected CPU and RAM configuration.
    """
        
    # Extended list of machine types
    machine_types = [
        {"name": "n1-standard-4", "cpu": 4, "ram": 15,
         "supported_gpu_machines": [
             {"name": "NVIDIA_TESLA_T4", "gpu_cores": [1, 2, 4]}, 
             {"name": "NVIDIA_TESLA_K80", "gpu_cores": [1, 2, 4, 8]}, 
             {"name": "NVIDIA_TESLA_P100", "gpu_cores": [1, 2, 4]},
             {"name": "NVIDIA_TESLA_V100", "gpu_cores": [1, 2, 4, 8]},
             {"name": "NVIDIA_TESLA_P4", "gpu_cores": [1, 2, 4]}
         ]
        },
        {"name": "n1-standard-8", "cpu": 8, "ram": 30,
         "supported_gpu_machines": [
             {"name": "NVIDIA_TESLA_T4", "gpu_cores": [1, 2, 4]}, 
             {"name": "NVIDIA_TESLA_K80", "gpu_cores": [1, 2, 4, 8]}, 
             {"name": "NVIDIA_TESLA_P100", "gpu_cores": [1, 2, 4]},
             {"name": "NVIDIA_TESLA_V100", "gpu_cores": [1, 2, 4, 8]},
             {"name": "NVIDIA_TESLA_P4", "gpu_cores": [1, 2, 4]}
         ]
        },
        {"name": "n1-standard-16", "cpu": 16, "ram": 60,
         "supported_gpu_machines": [
             {"name": "NVIDIA_TESLA_T4", "gpu_cores": [1, 2, 4]}, 
             {"name": "NVIDIA_TESLA_K80", "gpu_cores": [2, 4, 8]}, 
             {"name": "NVIDIA_TESLA_P100", "gpu_cores": [1, 2, 4]},
             {"name": "NVIDIA_TESLA_V100", "gpu_cores": [2, 4, 8]},
             {"name": "NVIDIA_TESLA_P4", "gpu_cores": [1, 2, 4]}
         ]
        },
        {"name": "n1-standard-32", "cpu": 32, "ram": 120,
         "supported_gpu_machines": [
             {"name": "NVIDIA_TESLA_T4", "gpu_cores": [2, 4]}, 
             {"name": "NVIDIA_TESLA_K80", "gpu_cores": [4, 8]}, 
             {"name": "NVIDIA_TESLA_P100", "gpu_cores": [2, 4]},
             {"name": "NVIDIA_TESLA_V100", "gpu_cores": [4, 8]},
             {"name": "NVIDIA_TESLA_P4", "gpu_cores": [2, 4]}
         ]
        },
        {"name": "n1-standard-64", "cpu": 64, "ram": 240,
         "supported_gpu_machines": [
             {"name": "NVIDIA_TESLA_T4", "gpu_cores": [4]}, 
             {"name": "NVIDIA_TESLA_V100", "gpu_cores": [8]},
             {"name": "NVIDIA_TESLA_P4", "gpu_cores": [4]}
         ]
        },
        {"name": "n1-standard-96", "cpu": 96, "ram": 360,
         "supported_gpu_machines": [
             {"name": "NVIDIA_TESLA_T4", "gpu_cores": [4]}, 
             {"name": "NVIDIA_TESLA_V100", "gpu_cores": [8]},
             {"name": "NVIDIA_TESLA_P4", "gpu_cores": [4]}
         ]
        },
        {"name": "n1-highmem-2", "cpu": 2, "ram": 13},
        {"name": "n1-highmem-4", "cpu": 4, "ram": 26},
        {"name": "n1-highmem-8", "cpu": 8, "ram": 52},
        {"name": "n1-highmem-16", "cpu": 16, "ram": 104},
        {"name": "n1-highmem-32", "cpu": 32, "ram": 208},
        {"name": "n1-highmem-64", "cpu": 64, "ram": 416},
        {"name": "n1-highmem-96", "cpu": 96, "ram": 624}
    ]

    warning_message = ''
    
    # Find the machine type closest to the requested CPU and RAM
    closest_machine = [machine for machine in machine_types if machine['ram'] >= selected_ram_gb and machine['cpu'] >= selected_cpu_cores and (("supported_gpu_machines" in machine and selected_gpu_type) or not selected_gpu_type)]
    if closest_machine:
        closest_machine = min(closest_machine, key=lambda x: (x['ram'], x['cpu']))
    else:
        closest_machine = max(machine_types, key=lambda x: (bool(("supported_gpu_machines" in x and selected_gpu_type) or not selected_gpu_type), x['ram']))
        warning_message = 'CPU error: there are no machines with the specified cores and ram. It will be used lower values.\n'
        
    closest_machine['cpu_machine_name'] = closest_machine.pop('name')
    closest_machine['cpu_machine_cores'] = closest_machine.pop('cpu')
    closest_machine['cpu_machine_ram'] = closest_machine.pop('ram')
            
    # Find the machine type closest to the requested GPU
    if selected_gpu_type:
        # selected_gpu_type must be in the supported gpu machines by closest cpu machine 
        if selected_gpu_type not in set([gpu_machine['name'] for gpu_machine in closest_machine['supported_gpu_machines']]):
            raise ValueError(f"GPU Type was not found or {selected_gpu_type} is not available for the specified CPU and RAM. It could be {list(set([gpu_machine['name'] for gpu_machine in closest_machine['supported_gpu_machines']]))}") 
        
        # Select supported gpu machines by closest cpu machine 
        closest_machine_supported_gpu_machines = closest_machine["supported_gpu_machines"]
        compatible_gpus_with_type = [gpu for gpu in closest_machine_supported_gpu_machines if gpu['name'] == selected_gpu_type][0]
            
        # Find the machine type closest to the requested GPU cores
        compatible_gpus_with_cores = [gpu_cores for gpu_cores in compatible_gpus_with_type['gpu_cores'] if gpu_cores >= selected_gpu_cores]
        if compatible_gpus_with_cores:
            compatible_gpus_with_cores = min(compatible_gpus_with_cores)
        else:
            compatible_gpus_with_cores = max(compatible_gpus_with_type['gpu_cores'])
            warning_message = warning_message+'GPU error: there are no machines with the specified cores. It will be used a lower value.\n'


        closest_machine['gpu_machine_name'] = compatible_gpus_with_type['name']
        closest_machine['gpu_machine_cores'] = compatible_gpus_with_cores
        closest_machine.pop('supported_gpu_machines')
        
    # Drop supported_gpu_machines
    if "supported_gpu_machines" in closest_machine:
        closest_machine.pop('supported_gpu_machines')
        
    # warning message to report changes to resource values
    if warning_message:
        warnings.warn(warning_message)

    return closest_machine


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cpu_cores',
        help='Number of CPU cores required.',
        type=int,
        required=True
    )
    parser.add_argument(
        "--ram_gb",
        help="Amount of RAM required in gigabytes.",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--gpu_cores", 
        help="Number of GPU cores required, to be provided with selected_gpu_type.", 
        type=int, 
        default=None
    )
    parser.add_argument(
        "--gpu_type", 
        help="Type of GPU required, to be provided with selected_gpu_cores.", 
        type=str, 
        default=None
    )
    
    args = parser.parse_args()
    
    # gpu_cores and gpu_type must have value both or neither
    if args.gpu_cores or args.gpu_type:
        if args.gpu_cores is None or args.gpu_type is None:
            raise ValueError('If you need GPU you have to set a value for "gpu_cores" and "gpu_type"')
    
    closest_machine = find_suitable_gcp_machine(
        args.cpu_cores, 
        args.ram_gb, 
        args.gpu_cores, 
        args.gpu_type
    )

    print(closest_machine)
