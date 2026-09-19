import requests

BOT_TOKEN = "8748123394:AAEh2V7p1S5eHP3WXocF50N3uVeAIdQjc_Y"
CHAT_ID = "8973390809"

def raise_alarm(message="ALARM server issue"):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    response = requests.post(
        url,
        data={
            "chat_id": CHAT_ID,
            "text": message
        }
    )

    response.raise_for_status()

    print("Message sent successfully")

# https://api.telegram.org/bot8748123394:AAEh2V7p1S5eHP3WXocF50N3uVeAIdQjc_Y/getUpdates