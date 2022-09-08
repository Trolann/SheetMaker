from config_db import config_db
from requests import get
from time import sleep

def get_site(url):
    try:
        return get(url)
    except Exception as e:
        get_site(url)

def heartbeat_daemon(delay, loop):
    # TODO: Add environment variable to RastaBot (Replit)
    get_site(config_db.heartbeat_url)
    while loop:
        get_site(config_db.heartbeat_url)
        sleep(delay)
