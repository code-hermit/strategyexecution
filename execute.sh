#!/usr/bin/env zsh
# ssh -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' ec2-user@3.109.123.94
# ssh -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' ec2-user@3.111.138.194


# scp -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' -r ec2-user@3.111.138.194:/home/ec2-user/trading/exec_rsv_adjust_sl_NIFTY.log ./logs

# scp -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' -r ec2-user@3.111.138.194:/home/ec2-user/trading/exec_rsv_adjust_sl_SENSEX.log ./logs

# scp -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' -r ec2-user@3.111.138.194:/home/ec2-user/trading/logs/sensex_option_buying.log ./logs
scp -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' -r ec2-user@3.111.138.194:/home/ec2-user/trading/exec_rsv_goldm_GOLDM.log ./logs