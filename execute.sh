#!/usr/bin/env zsh
ssh -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' ec2-user@3.111.138.194


# scp -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' -r ec2-user@3.111.138.194:/home/ec2-user/trading/logs .