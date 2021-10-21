from os import getenv
from datetime import datetime, timedelta
import pandas as pd
from discord import Webhook, RequestsWebhookAdapter
import psycopg2

# the time difference (in days) between the start and end dates
START_DATE_TIME_DELTA = 7


def lambda_handler():
    # set the start and end dates
    end_date = datetime.today()
    start_date = end_date - timedelta(days=START_DATE_TIME_DELTA)

    end_date_query = end_date.strftime("%Y-%m-%d")
    start_date_query = start_date.strftime("%Y-%m-%d")

    end_date_display = end_date.strftime("%d %B, %Y")
    start_date_display = start_date.strftime("%d %B, %Y")

    # create connection to the database
    db_params = {
        "host": getenv("host"),
        "database": getenv("database"),
        "user": getenv("user"),
        "password": getenv("password"),
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # get the list of all organisations
    cursor.execute("select schema_name, name from organization")
    organisations = cursor.fetchall()

    # create a mapping between schema name and organisation name
    org_schema_to_name = {
        organisation[0]: organisation[1] for organisation in organisations
    }
    schemas = [organisation[0] for organisation in organisations]

    organisation_labels = []
    average_watch_times = []
    numbers_of_active_users = []
    numbers_of_plios_viewed = []

    df_columns = [
        "Average Watch Time (minutes) 🦾",
        "Number of Active Users 🚀",
        "Number of Plios viewed 🥽",
    ]
    results_df = pd.DataFrame(columns=df_columns)

    for schema in schemas:
        query = f"""WITH summary AS(
            select
                session.plio_id,
                session.user_id,
                session.watch_time,
                session.created_at,
                ROW_NUMBER() OVER(PARTITION BY session.user_id, session.plio_id
                            ORDER BY session.watch_time DESC) AS rank
                from "{schema}"."session"
            )
            select
                sum(ROUND(watch_time::numeric, 2)) / 60,
                count(DISTINCT(user_id)), 
                count(DISTINCT(plio_id))
                from summary
            WHERE
                date(created_at) between '{start_date_query}' and '{end_date_query}'
                and watch_time > 0
                and rank = 1"""

        cursor.execute(query)
        results = cursor.fetchone()

        total_watch_time, num_active_users, num_plios_viewed = results

        # only proceed if the schema has any active users
        if not num_active_users:
            continue

        average_watch_time = round(total_watch_time / num_active_users, 2)

        if schema == "public":
            organisation_labels.append("Personal Workspace")
        else:
            organisation_labels.append(org_schema_to_name[schema])

        average_watch_times.append(average_watch_time)
        numbers_of_active_users.append(num_active_users)
        numbers_of_plios_viewed.append(num_plios_viewed)

    results_df[df_columns[0]] = average_watch_times
    results_df[df_columns[1]] = numbers_of_active_users
    results_df[df_columns[2]] = numbers_of_plios_viewed
    results_df.index = organisation_labels

    # prepare the final message to be sent
    final_results = f"**{start_date_display} - {end_date_display}**\n\n"

    for column in df_columns:
        final_results += f"**{column}**\n"
        final_results += results_df[column].to_string() + "\n"
        final_results += "\n"

    cursor.execute("select COUNT(*) from public.user")
    num_users = cursor.fetchone()[0]

    final_results += f"**Number of Registered Users**: {num_users}"

    webhook = Webhook.from_url(
        getenv("webhook"),
        adapter=RequestsWebhookAdapter(),
    )
    webhook.send(final_results)
    print("Message Successful")

    conn.close()


if __name__ == "__main__":
    from configparser import ConfigParser
    from os import environ
    from os.path import exists

    config_filename = "config.ini"

    # check whether the config file exists
    if not exists(config_filename):
        raise FileNotFoundError(
            f"{config_filename} is not present. Please create a config file by following the README before proceeding!"
        )

    parser = ConfigParser()
    parser.read(config_filename)

    # check whether the required sections exist within the config file
    required_sections = ["database", "discord"]
    for section_name in required_sections:
        if not parser.has_section(section_name):
            raise ValueError(
                f"Section {section_name} is not present in the config file. Please follow the README to set the correct values!"
            )

    def check_required_params_specified(section_name, required_params):
        section_params = parser.items(section_name)
        specified_params = set()

        for param in section_params:
            specified_params.add(param[0])

        for param in required_params:
            if param not in specified_params:
                raise ValueError(
                    f"Missing parameter {param} under section {section_name}"
                )

    # check if the required keys are present in each section
    check_required_params_specified(
        "database", ["host", "database", "user", "password"]
    )
    check_required_params_specified("discord", ["webhook"])

    def set_env_variables_from_config(section_name):
        section_params = parser.items(section_name)
        for key, value in section_params:
            environ[key] = value

    for section_name in required_sections:
        set_env_variables_from_config(section_name)

    lambda_handler()
