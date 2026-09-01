import base64
import re

import boto3
import os

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")
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
    """

    ip_address = (event or {}).get("ip_address", DEFAULT_IP_ADDRESS)

    instance_id = find_instance_id_by_ip(ip_address)
    if not instance_id:
        print(f"no running instance found with ip {ip_address}")
        return {"status": "not_found", "ip_address": ip_address}

    if not is_ssm_ready(instance_id):
        print(f"instance {instance_id} not yet visible to SSM")
        return {"status": "waiting_for_ssm", "instance_id": instance_id}

    print(f"got ssm for {instance_id}, sending commands")
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
                "cd /home/ec2-user/trading && python3 dhan_generate_access_token.py",
                # Everything up to here ran as root (SSM's default user), so
                # /home/ec2-user/trading and the files inside it (including
                # .env) are root-owned. Hand it back to ec2-user before wiring
                # up cron, so the scripts can actually read .env when they run.
                "chown -R ec2-user:ec2-user /home/ec2-user/trading",
                "chmod +x /home/ec2-user/trading/exec_rsv_cont.sh "
                "/home/ec2-user/trading/exec_rs_ps.sh "
                "/home/ec2-user/trading/sensex_buying.sh "
                "/home/ec2-user/trading/zerodha_ticker_service.sh",
                # One cron line per weekday's strategy, rather than one daily line for
                # exec_rsv_cont.sh - each script also refuses to trade outside its own
                # day(s) if invoked on the wrong day, but restricting the cron
                # day-of-week field too means the wrong tmux session never even spins up:
                #   Mon-Fri (1,2,3,4,5)         9:42 -> zerodha_ticker_service.sh (started first,
                #                                       so its Redis feed is already warm by 9:45)
                #   Mon/Wed/Thu/Fri (1,3,4,5)   9:45 -> exec_rsv_cont.sh
                #   Tue (2)                     9:45 -> exec_rs_ps.sh
                #   All days                   10:15 -> sensex_buying.sh
                (
                    "crontab -u ec2-user -l 2>/dev/null | grep -q option_selling || "
                    "(crontab -u ec2-user -l 2>/dev/null; "
                    "echo \"42 9 * * 1,2,3,4,5 /usr/bin/tmux new-session -d -s zerodha_ticker '/home/ec2-user/trading/zerodha_ticker_service.sh'\"; "
                    "echo \"45 9 * * 1,3,4,5 /usr/bin/tmux new-session -d -s option_selling '/home/ec2-user/trading/exec_rsv_cont.sh'\"; "
                    "echo \"45 9 * * 2 /usr/bin/tmux new-session -d -s option_selling_tn '/home/ec2-user/trading/exec_rs_ps.sh'\"; "
                    "echo \"15 10 * * * /usr/bin/tmux new-session -d -s sensex_buying '/home/ec2-user/trading/sensex_buying.sh'\") | crontab -u ec2-user -"
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
