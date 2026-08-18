import requests
import json
import pyotp
import time
import os
from dotenv import load_dotenv
load_dotenv()
def generate_token():
    CLIENT_ID  = os.getenv('DHAN_CLIENT_ID')
    API_KEY    = os.getenv('DHAN_API_KEY')
    API_SECRET = os.getenv('DHAN_API_SECRET')
    TOTP_KEY   = os.getenv('DHAN_TOTP_KEY')
    PIN        = os.getenv('DHAN_PIN')
    TOKEN_URL = "https://api.dhan.co/v2/token"
    TOTP=pyotp.TOTP(TOTP_KEY).now()


    accesstoken_url=f'https://auth.dhan.co/app/generateAccessToken?dhanClientId={CLIENT_ID}&pin={PIN}&totp={TOTP}'
    resp=requests.post(accesstoken_url)
    token_data=resp.json()

        # Persist token so other scripts can read it
    if 'accessToken' in token_data:
        with open("dhan_token.json", "w") as f:
            json.dump(token_data, f, indent=2)
        print("Saved to dhan_token.json")
        return token_data
    return token_data

def generate_and_store_token():
    token_data=generate_token()
    print(token_data)
    if 'status' in token_data and token_data['status'] == 'error':
        time.sleep(130)
        generate_and_store_token()

if __name__ == "__main__":
    generate_and_store_token()