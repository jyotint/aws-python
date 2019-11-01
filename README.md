# Overview
Interact with AWS Services. Application is written using Python v3 and boto3 AWS library for Python.

# Setup
### Pre-requistes
* Python v3.7.4

### Install AWS CLI
Follow the instruction at below mentioned link if [AWS CLI](https://docs.aws.amazon.com/en_pv/cli/latest/userguide/cli-chap-install.html) is not installed

### Configure AWS account using following steps
* Create Security Access Key for user with appropriate permissions
* Run following command to configure security access key generated from previous step "aws configure"

## Third Party Libraries
Install following Python libraries
* click
* coloredlogs
* pyyaml
* boto3

# Run AWS Services
### Interact with AWS SQS (Simple Queue Service)
`python main_sqs.py --help`

### Interact with AWS SNS (Simple Notification Service)
`python main_sns.py --help`
