# Plio Usage Statistics Discord Bot

This repository contains the code for a discord bot which periodically generates usage statistics for plio and sends them to a discord channel.

## Setup
1. Copy the `config.example.ini` file to `config.ini` and fill the respective values. The meaning of the sections and parameters can be found in `docs/ENV.md`.
2. Set up a virtual environment and install the dependencies
   
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
3. Run the script

    ```bash
    python lambda_function.py
    ```

## Deployment
To make this script run periodically, there are two options.
1. Set up a cron job on an EC2 instance
2. Set up an AWS Lambda function which is trigged to run periodically

Since we already had an EC2 instance for running other cron jobs and the fact that it is a bit painful to make pandas/numpy work
with Lambda, we have currently gone ahead with option 1.