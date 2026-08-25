import os
command=f'scp -i "/Users/subhash/AWS/AWS/aws keys/zct.pem" file_name ec2-user@3.111.138.194:~/trading'
for file in [k for k in os.listdir('./') if k.endswith('.json') or k.endswith('.py') or k.endswith('.env') or k.endswith('.txt')]:
    # if ".py" in file or ".env" in file:
    if "mcx_option_buying.py" in file or "mcx_short_straddle_premium_stoploss.py" in file:
        os.system(command.replace('file_name', file))