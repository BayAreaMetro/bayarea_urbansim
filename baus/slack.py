from __future__ import print_function

import orca


def slack_start(run_mode, host, run_name, run_setup):
    """
    Message to slack that the BAUS run has started.
    """
    slack_start_message = f'Starting BAUS with mode {run_mode}: {run_name} on host {host}\nOutput written to: {run_setup["outputs_dir"]}'
    try:
        import slack_sdk
        import slack_sdk.errors
        slack_client = orca.get_injectable('slack_client')
        slack_channel = orca.get_injectable('slack_channel')
        init_response = slack_client.chat_postMessage(channel=slack_channel, text=slack_start_message)
        orca.add_injectable('slack_init_response', init_response)

    except slack_sdk.errors.SlackApiError as e:
        print(f"Slack Channel Connection Error: {e.response['error']}")
        assert e.response["ok"] is False
        assert e.response["error"]  

def slack_complete(run_mode, host, run_name):
    """
    Message to slack that the BAUS run is complete.
    """
    slack_completion_message = f'Completed BAUS with mode {run_mode}: {run_name} on host {host}'
    try:
        import slack_sdk
        import slack_sdk.errors
        slack_client = orca.get_injectable('slack_client')
        slack_channel = orca.get_injectable('slack_channel')
        slack_init_response = orca.get_injectable('slack_init_response')
        response = slack_client.chat_postMessage(channel=slack_channel,
                                       thread_ts=slack_init_response.data['ts'],
                                       text=slack_completion_message)
    except slack_sdk.errors.SlackApiError as e:
        print(f"Slack Channel Connection Error: {e.response['error']}")

def slack_error(error_type, error_msg, error_trace):
    """
    Message to slack that there was a problem with the run
    """
    slack_fail_message = f'DANG!  Run failed with error_type="{error_type}", error_msg="{error_msg}". Deets here:\n{error_trace}'
    import slack_sdk
    slack_client = orca.get_injectable('slack_client')
    slack_channel = orca.get_injectable('slack_channel')
    slack_init_response = orca.get_injectable('slack_init_response')
    response = slack_client.chat_postMessage(channel=slack_channel, 
                                             thread_ts=slack_init_response.data['ts'], 
                                             text=slack_fail_message)
    # don't worry about failure

@orca.step()
def slack_simulation_status(year, slack_client, slack_channel, slack_init_response):
    """
    year: model year
    client: slack.WebClient instance
    slack_channel: str
    slack_init_response: 

    See https://slack.dev/python-slack-sdk/web/index.html
    """
    print(f"slack_simulation_status called() with year, slack_client, slack_channel, slack_init_response:")
    print(type(slack_init_response))

    try:
        import slack_sdk
        import slack_sdk.errors
        response = slack_client.chat_postMessage(
            channel=slack_channel,
            thread_ts=slack_init_response.data['ts'],
            text=f'Completed simulation year {year}')
    except slack_sdk.errors.SlackApiError as e:
        print(f"Slack Channel Connection Error: {e.response['error']}")
        assert e.response["ok"] is False
        assert e.response["error"]
    return