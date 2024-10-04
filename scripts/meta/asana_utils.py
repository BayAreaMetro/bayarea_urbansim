import logging
import asana
import logging
from datetime import datetime, timedelta
import holidays
import yaml
import html
import os

# CONSTANTS - setting as env variables for now
ASANA_PERSONAL_ACCESS_TOKEN = os.getenv('ASANA_TOKEN')
WORKSPACE_ID = os.getenv('WORKSPACE_ID')
URBAN_MODELING_PROJECT_ID = os.getenv('URBAN_MODELING_PROJECT_ID')
PBA50_PROJECT_ID = os.getenv('PBA50_PROJECT_ID')
ASSIGNEE_EMAIL = f'{os.getlogin()}@bayareametro.gov'

# Set up Asana client object
client = asana.Client.access_token(ASANA_PERSONAL_ACCESS_TOKEN)


import logging

# Obtain the logger configured in the main script
logger = logging.getLogger(__name__)


def create_asana_task(client, workspace_id, task_name, project_id, section_id, description, assignee_email):
    """Creates a task in Asana with the provided details."""
    try:
        # Calculate the due date (+1 business day after creation)
        us_holidays = holidays.US()
        creation_date = datetime.today()
        due_date = creation_date + timedelta(days=1)

        # Adjust due date if it falls on a weekend or holiday
        while due_date.weekday() >= 5 or due_date in us_holidays:
            due_date += timedelta(days=1)

        due_date_str = due_date.strftime('%Y-%m-%d')

        # Task payload
        task_payload = {
            'name': task_name,
            'projects': [project_id],
            'memberships': [{'project': project_id, 'section': section_id}],
            'assignee': assignee_email,
            'due_on': due_date_str,
            'html_notes': description
        }

        task_handle = client.tasks.create_in_workspace(workspace_id, task_payload)
        logger.info(f"Task '{task_name}' created successfully with ID: {task_handle['gid']}")
        return task_handle
    except Exception as e:
        logger.error(f"Error creating task '{task_name}': {e}")
        raise

def yaml_loader(filename):
    """Loads and returns the content of a YAML file."""
    try:
        with open(filename, 'r') as f:
            data = yaml.safe_load(f)
        return data
    except Exception as e:
        logger.error(f"Error loading YAML file '{filename}': {e}")
        raise

def generate_description_from_yaml(setup):
    """Generates an HTML-formatted description from the YAML setup."""
    annotation = setup.get('annotation', [])
    html_annotation = generate_html_list(annotation) if annotation else ''

    description = f"<body>{html_annotation}" +\
                  f"<h2>Inputs</h2><ul><li>{setup.get('inputs_dir', '')}</li></ul>" +\
                  f"<h2>Outputs</h2><ul><li>{setup.get('outputs_dir', '')}/{setup.get('run_name', '')}</li></ul></body>"
    return description

def generate_html_list(items):
    """Generates an unordered HTML list from a list of items."""
    header = "<h2>Features (from YAML annotation)</h2>"
    html_list = "<ul>"
    for item in items:
        html_list += f"<li>{html.escape(item)}</li>"
    html_list += "</ul>"
    return header+html_list

def get_or_create_section(client, project_id, section_name):
    """Check if a section exists in the project, and create it if not."""
    try:
        sections = client.sections.get_sections_for_project(project_id)
        for section in sections:
            if section['name'].lower() == section_name.lower():
                logger.info(f"Section '{section_name}' already exists with ID: {section['gid']}")
                return section['gid']

        # Section not found, create it
        new_section = client.sections.create_section_for_project(project_id, {'name': section_name})
        logger.info(f"Section '{section_name}' created with ID: {new_section['gid']}")
        return new_section['gid']

    except Exception as e:
        logger.error(f"Error checking or creating section '{section_name}': {e}")
        raise

def create_asana_task_from_yaml(yaml_path, task_name, section_name):
    """Main function to create an Asana task based on the BAUS YAML run_setup file."""
    setup = yaml_loader(yaml_path)
    description = generate_description_from_yaml(setup)

    # Log the task details
    logger.info(f"YAML path: {yaml_path}")
    logger.info(f"Task name: {task_name}")
    logger.info(f"Section name: {section_name}")

    # Check if section exists, and create it if it doesn't
    section_id = get_or_create_section(client, PBA50_PROJECT_ID, section_name)

    try:
        task_handle = create_asana_task(client, WORKSPACE_ID, task_name, PBA50_PROJECT_ID, section_id, description, ASSIGNEE_EMAIL)
        logger.info(f"Task created with ID: {task_handle['gid']}")
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        raise

    return task_handle

def add_comment_to_task(task_gid, comment):
    try:
        comment_payload = {
            'text': comment
        }
        client.tasks.add_comment(task_gid, comment_payload)
        logger.info(f"Comment added to task with ID: {task_gid}")
    except Exception as e:
        logger.error(f"Error adding comment to task '{task_gid}': {e}")
        raise


def mark_task_as_complete(task_gid):
    try:
        client.tasks.update(task_gid, {'completed': True})
        logger.info(f"Task with ID: {task_gid} marked as completed.")
    except Exception as e:
        logger.error(f"Error marking task '{task_gid}' as completed: {e}")
        raise
