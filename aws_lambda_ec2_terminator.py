import os
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

ec2 = boto3.client("ec2")

IST = ZoneInfo("Asia/Kolkata")
CUTOFF_HOUR = 16  # 16:00 IST

# The Elastic IP of the instance to terminate.
ELASTIC_IP = os.environ.get('ELASTIC_IP')


def lambda_handler(event, context):
    """Terminates the EC2 instance currently associated with ELASTIC_IP, but
    only if run at or after 16:00 IST. Exits quietly (no-op) if run earlier,
    so it's safe to schedule this to run/retry ahead of the cutoff.
    """

    now_ist = datetime.now(IST)
    if now_ist.hour < CUTOFF_HOUR:
        print(f"current time {now_ist.isoformat()} is before {CUTOFF_HOUR}:00 IST, exiting without action")
        return {"status": "too_early", "current_time_ist": now_ist.isoformat()}

    instance_id = find_instance_id_by_ip(ELASTIC_IP)
    if not instance_id:
        print(f"no running instance found with ip {ELASTIC_IP}")
        return {"status": "not_found", "ip_address": ELASTIC_IP}

    print(f"terminating instance {instance_id} ({ELASTIC_IP})")
    ec2.terminate_instances(InstanceIds=[instance_id])

    return {"status": "terminated", "instance_id": instance_id}


def find_instance_id_by_ip(ip_address):
    response = ec2.describe_instances(
        Filters=[
            {"Name": "ip-address", "Values": [ip_address]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            return instance["InstanceId"]
    return None
