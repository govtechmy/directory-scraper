import requests

def send_discord_notification(message, webhook_url, thread_id=None):
    """
    Sends a notification to a Discord channel or thread.
    
    Args:
        message (str): The message to send.
        webhook_url (str): The Discord webhook URL.
        thread_id (str, optional): The ID of the thread where the message should be sent.
    """
    # Append thread ID to webhook URL if provided
    if thread_id:
        webhook_url += f"?thread_id={thread_id}"
    
    payload = {"content": message}
    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 204:
            print("Notification sent to Discord successfully.")
        else:
            print(f"Failed to send notification. Status code: {response.status_code}. Response: {response.text}")
    except Exception as e:
        print(f"Error sending notification to Discord: {e}")
