import boto3
import time

ec2 = boto3.client("ec2")

# Instance profile granting the EC2 instance SSM access (AmazonSSMManagedInstanceCore).
# The Lambda's execution role needs iam:PassRole on this profile's role ARN to hand
# it off at launch time.
IAM_INSTANCE_PROFILE_NAME = "ec2-user"


def lambda_handler(event, context):
    """Launches the EC2 instance and associates the Elastic IP, then returns
    immediately. Does NOT wait for SSM — see aws_lambda_ssm_runner.py for that.
    """

    response = ec2.run_instances(
        ImageId="ami-0ac7b260cf76d8865",
        InstanceType="t3.micro",
        MinCount=1,
        MaxCount=1,

        KeyName="zct",

        IamInstanceProfile={"Name": IAM_INSTANCE_PROFILE_NAME},

        NetworkInterfaces=[
            {
                "DeviceIndex": 0,
                "Groups": ["sg-0802c40b86fd249b6"],
                "AssociatePublicIpAddress": False,
                "DeleteOnTermination": True
            }
        ]
    )

    instance_id = response["Instances"][0]["InstanceId"]

    # Elastic IP association can fail transiently right after launch
    # (instance not yet in a state that accepts it), so retry a few times.
    ip_not_assigned = True
    attempts = 0
    while ip_not_assigned and attempts < 12:  # up to ~1 minute
        try:
            ec2.associate_address(
                AllocationId="eipalloc-07a217676a4924822",
                InstanceId=instance_id
            )
            ip_not_assigned = False
        except Exception as e:
            attempts += 1
            print(f"ip assignment failed (attempt {attempts}): {e}")
            time.sleep(5)

    if ip_not_assigned:
        raise Exception("Could not associate Elastic IP after several attempts")

    print(f"launched instance {instance_id}")
    return {
        "instance_id": instance_id
    }
