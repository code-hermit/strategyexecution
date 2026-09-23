import base64
import logging
import re

import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")
# Optional: set this to an SNS topic ARN to get notified when a scheduled run
# fails outright (as opposed to "not_found"/"waiting_for_ssm", which are
# expected states while the instance boots). Left unset, failures are still
# logged and the Lambda invocation is still marked as an error (so a
# CloudWatch alarm on the function's Errors metric will catch it too) -
# this is just a more immediate/direct notification if you want one.
alert_topic_arn = os.environ.get('ALERT_SNS_TOPIC_ARN')
sns = boto3.client("sns") if alert_topic_arn else None
github_pat = os.environ.get('GITHUB_PAT')

# The Elastic IP this Lambda should look for. Override via event["ip_address"]
# if you'd rather pass it in than hardcode it.
DEFAULT_IP_ADDRESS = os.environ.get('ELASTIC_IP')

# The whole .env file content, pasted as a single Lambda environment variable.
# Never commit .env to git — this is read from the Lambda's env and written
# to the instance at deploy time.
ENV_VAR = os.environ.get('ENV_VAR', '')


# Matches KEY = 'value' or KEY = "value" pairs regardless of whether entries
# are separated by newlines or spaces, and regardless of spacing around '='.
ENV_PAIR_RE = re.compile(r"""([A-Za-z_][A-Za-z0-9_]*)\s*=\s*['"]([^'"]*)['"]""")


def normalize_env_content(raw):
    """Turns pasted .env content (possibly flattened to spaces instead of
    newlines, e.g. by the Lambda console) back into proper KEY=value lines.
    """
    pairs = ENV_PAIR_RE.findall(raw)
    lines = [f"{key}={value}" for key, value in pairs]
    return "\n".join(lines) + "\n"


def build_env_file_command(remote_path):
    """Builds a shell command that writes a .env file on the instance from
    this Lambda's own ENV_VAR content. Base64-encoded so secret values with
    quotes/special characters can't break the shell command, and so the
    values don't sit in plain, easily-greppable text in the SSM command
    parameters themselves.
    """
    content = normalize_env_content(ENV_VAR)
    encoded = base64.b64encode(content.encode()).decode()
    return (
        f"echo {encoded} | base64 -d > {remote_path} && chmod 600 {remote_path}"
    )


def lambda_handler(event, context):
    """Finds the instance by its (Elastic) IP, checks SSM readiness once, and
    runs the setup commands if SSM is up.

    Designed to be invoked repeatedly (e.g. by an EventBridge schedule every
    1 minute) instead of sleeping inside the Lambda. Returns a status of
    "not_found", "waiting_for_ssm", or "commands_sent" so the caller knows
    whether to try again.

    Any unexpected exception (e.g. a boto3/IAM/throttling error) is logged
    with its full traceback, optionally pushed to SNS if ALERT_SNS_TOPIC_ARN
    is set, and then re-raised - so the invocation still shows up as a
    failure in CloudWatch/the Lambda's Errors metric instead of the schedule
    just quietly not doing anything that run.
    """

    ip_address = (event or {}).get("ip_address", DEFAULT_IP_ADDRESS)

    try:
        return _run(ip_address)
    except Exception:
        logger.exception(f"aws_lambda_ssm_runner failed for ip {ip_address}")
        _notify_failure(ip_address)
        raise


def _notify_failure(ip_address):
    if not sns:
        return
    try:
        sns.publish(
            TopicArn=alert_topic_arn,
            Subject="aws_lambda_ssm_runner failed",
            Message=f"aws_lambda_ssm_runner failed for ip {ip_address}. See CloudWatch Logs for the traceback.",
        )
    except Exception:
        # Don't let a broken notification path mask the original error.
        logger.exception("also failed to publish failure notification to SNS")


def _run(ip_address):
    instance_id = find_instance_id_by_ip(ip_address)
    if not instance_id:
        logger.info(f"no running instance found with ip {ip_address}")
        return {"status": "not_found", "ip_address": ip_address}

    if not is_ssm_ready(instance_id):
        logger.info(f"instance {instance_id} not yet visible to SSM")
        return {"status": "waiting_for_ssm", "instance_id": instance_id}

    logger.info(f"got ssm for {instance_id}, sending commands")
    ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={
            "commands": [
                "timedatectl set-timezone Asia/Kolkata",
                "dnf install -y python3 python3-pip git",
                "sudo dnf install -y tmux",
                "dnf install -y cronie",
                "systemctl enable --now crond",
                # Redis backs zerodha_ltp_client.py's shared LTP cache (see zerodha_ticker_service.py) -
                # redis6 is Amazon Linux 2023's package name; fall back to plain "redis" for any other
                # dnf-based AMI. Binds to localhost by default (the package's stock config), which is
                # all every script here needs - they all run on this same instance.
                "dnf install -y redis6 || dnf install -y redis",
                "systemctl enable --now redis6 2>/dev/null || systemctl enable --now redis",
                "python3 -m pip install --upgrade pip",
                "mkdir -p /home/ec2-user/trading",
                f"git clone https://{github_pat}@github.com/code-hermit/strategyexecution.git /home/ec2-user/trading",
                build_env_file_command("/home/ec2-user/trading/.env"),
                "cd /home/ec2-user/trading && pip install -r requirements.txt",
 
                # Everything up to here ran as root (SSM's default user), so
                # /home/ec2-user/trading and the files inside it (including
                # .env) are root-owned. Hand it back to ec2-user before wiring
                # up cron, so the scripts can actually read .env when they run.
                "chown -R ec2-user:ec2-user /home/ec2-user/trading",
                "chmod +x /home/ec2-user/trading/exec_rsv_cont.sh "
                "/home/ec2-user/trading/exec_rsv_cont_sensex.sh "
                "/home/ec2-user/trading/sensex_buying.sh "
                "/home/ec2-user/trading/zerodha_ticker_service.sh "
                "/home/ec2-user/trading/process_monitor.sh",
                # One cron line per strategy, restricted to weekdays (1-5) at the day-of-week
                # field too so the wrong tmux session never even spins up on a weekend:
                #   Mon-Fri (1,2,3,4,5)   9:42 -> zerodha_ticker_service.sh (started first, so its
                #                                 Redis feed is already warm by 9:45)
                #   Mon-Fri (1,2,3,4,5)   9:44 -> exec_rsv_cont.sh (NIFTY, 2 lots - process starts a
                #                                 minute early to warm its own instrument/contract
                #                                 caches; exec_rsv_cont_chop.py's own ENTRY_TIME
                #                                 (9:45) still gates the actual entry snapshot/orders)
                #   Mon-Fri (1,2,3,4,5)   9:44 -> exec_rsv_cont_sensex.sh (SENSEX, 2 lots -
                #                                 both underlyings now trade every weekday)
                #   All days             10:14 -> sensex_buying.sh (process starts a minute early to
                #                                 warm its own instrument caches; sensex_option_
                #                                 buying.py's own ENTRY_TIME (10:15) still gates the
                #                                 actual checkpoint/entry)
                #   Mon-Fri (1,2,3,4,5)   9:46 -> process_monitor.sh (watches the above jobs and
                #                                 alarms if one dies; started last, after the jobs
                #                                 it watches are already up)
                # Each job is added independently, keyed on its own exact "-s <session-name> " tmux
                # flag (not a loose substring like "option_selling", which also matches
                # "option_selling_sensex") - a single shared "does crontab mention option_selling at
                # all" guard used to gate ALL four lines at once, so an instance whose crontab was
                # provisioned by an older version of this script (e.g. before sensex_buying.sh or
                # zerodha_ticker_service.sh had cron lines here) would see that guard already
                # satisfied by its existing option_selling line and skip adding the newer/missing
                # ones forever, on every subsequent run.
                (
                    "crontab -u ec2-user -l 2>/dev/null | grep -q -- '-s zerodha_ticker ' || "
                    "(crontab -u ec2-user -l 2>/dev/null; "
                    "echo \"42 9 * * 1,2,3,4,5 /usr/bin/tmux new-session -d -s zerodha_ticker '/home/ec2-user/trading/zerodha_ticker_service.sh'\") | crontab -u ec2-user -"
                ),
                (
                    "crontab -u ec2-user -l 2>/dev/null | grep -q -- '-s option_selling ' || "
                    "(crontab -u ec2-user -l 2>/dev/null; "
                    "echo \"44 9 * * 1,2,3,4,5 /usr/bin/tmux new-session -d -s option_selling '/home/ec2-user/trading/exec_rsv_cont.sh'\") | crontab -u ec2-user -"
                ),
                (
                    "crontab -u ec2-user -l 2>/dev/null | grep -q -- '-s option_selling_sensex ' || "
                    "(crontab -u ec2-user -l 2>/dev/null; "
                    "echo \"44 9 * * 1,2,3,4,5 /usr/bin/tmux new-session -d -s option_selling_sensex '/home/ec2-user/trading/exec_rsv_cont_sensex.sh'\") | crontab -u ec2-user -"
                ),
                (
                    "crontab -u ec2-user -l 2>/dev/null | grep -q -- '-s sensex_buying ' || "
                    "(crontab -u ec2-user -l 2>/dev/null; "
                    "echo \"14 10 * * * /usr/bin/tmux new-session -d -s sensex_buying '/home/ec2-user/trading/sensex_buying.sh'\") | crontab -u ec2-user -"
                ),
                (
                    "crontab -u ec2-user -l 2>/dev/null | grep -q -- '-s process_monitor ' || "
                    "(crontab -u ec2-user -l 2>/dev/null; "
                    "echo \"46 9 * * 1,2,3,4,5 /usr/bin/tmux new-session -d -s process_monitor '/home/ec2-user/trading/process_monitor.sh'\") | crontab -u ec2-user -"
                ),

            ]
        }
    )

    return {"status": "commands_sent", "instance_id": instance_id}


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


def is_ssm_ready(instance_id):
    response = ssm.describe_instance_information(
        Filters=[
            {
                "Key": "InstanceIds",
                "Values": [instance_id]
            }
        ]
    )
    return bool(response["InstanceInformationList"])
