import argparse
import logging
import pathlib
from asana_utils import create_asana_task_from_yaml
from logging_config import setup_logging


def main():


    # Set up logging configuration
    setup_logging()

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Create an Asana task from a YAML file.')

    # Adding arguments with defaults
    parser.add_argument(
        '-p','--yaml_path', 
        default='/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/PBA50Plus_FinalBlueprint/PBA50Plus_Final_Blueprint_v00/run_setup_PBA50Plus_Final_Blueprint_v00.yaml',
        help='Path to the YAML file with simulation setup details'
    )
    parser.add_argument(
        '-t','--task_name', 
        default='Example Task X',
        help='Name of the Asana task to create'
    )
    parser.add_argument(
        '-s','--section_name', 
        default='Final Blueprint Runs',
        help='Name of the section in the Asana project'
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Log the received arguments
    logger.info(f"YAML Path: {args.yaml_path}")
    logger.info(f"Task Name: {args.task_name}")
    logger.info(f"Section Name: {args.section_name}")

    run_name = f"BAUS Run: {pathlib.Path(args.yaml_path).name.replace('run_setup_', '').split('.')[0]}"

    # Call the function to create the Asana task
    try:
        task_handle = create_asana_task_from_yaml(args.yaml_path, run_name, args.section_name)
        logger.info(f"Task created successfully with ID: {task_handle['gid']}")
    except Exception as e:
        logger.error(f"Failed to create task: {e}")

if __name__ == "__main__":
    main()
