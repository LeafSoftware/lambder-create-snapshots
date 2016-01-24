# lambder-create-snapshots

create-snapshots is an AWS Lambda function for use with Lambder.

## REQUIRES:
* python-lambder

This lambda function creates an EBS snapshot from each EBS volume
tagged with Key: 'LambderBackup'. The function will retain at most 3 snapshots
and delete the oldest snapshot to stay under this threshold.

## Installation

1. Clone this repo
2. `cp example_lambder.json  lambder.json`
3. Edit lambder.json to set your S3  bucket
4. `lambder function deploy`

## Usage

Schedule the function with a new event. Rember that the cron expression is
based on UTC.

    lambder events add \
      --name CreateSnapshots \
      --function-name Lambder-create-snapshots \
      --cron 'cron(0 6 ? * * *)'

## TODO

* Parameterize the tag in the input event object
* Parameterize number of old snapshots to retain
