#!/usr/bin/env zsh
# ssh -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' ec2-user@3.109.123.94
# ssh -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' ec2-user@3.111.138.194

# Each log is saved as ./logs/<name>_<YYYY-MM-DD>.log, so earlier days are never overwritten. The date is
# taken from the log's first timestamped line (the day the log is for), falling back to today. Downloading
# again on the same day replaces that day's file with the newer, longer copy.

KEY='/Users/subhash/AWS/AWS/aws keys/zct.pem'
HOST='ec2-user@3.111.138.194'
REMOTE='/home/ec2-user/trading'

mkdir -p ./logs
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

download() {
    local remote_path=$1
    local file=${remote_path:t}  # e.g. hedge_buyer.log
    if ! scp -i "$KEY" "$HOST:$REMOTE/$remote_path" "$TMP/$file"; then
        echo "FAILED: $remote_path"
        return
    fi
    local day=$(grep -m1 -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2}' "$TMP/$file")
    [[ -z $day ]] && day=$(date +%F)
    local dest="./logs/${file:r}_${day}.log"
    mv "$TMP/$file" "$dest"
    echo "saved $dest"
}

download exec_rsv_adjust_sl_NIFTY.log
download exec_rsv_adjust_sl_SENSEX.log
download logs/sensex_option_buying.log
download logs/hedge_buyer.log
download stock_range_reversal_data/stock_range_reversal.log
download exec_rsv_goldm_GOLDM.log


# scp -i '/Users/subhash/AWS/AWS/aws keys/zct.pem' -r ec2-user@3.111.138.194:/home/ec2-user/trading/logs ./logs
