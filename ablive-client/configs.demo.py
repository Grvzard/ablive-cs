SERVER_API_KEY = "secret"


ADD_ROOM_INTERVAL = 0.2

THRD_PER_PROC = 1
THRD_TOTAL = 4

MACHINE_ID = '00'

TESTING = True

if TESTING:
    ABLIVE_SERVER_URL = "http://localhost"
    MY_DB_CONFIG = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": ""
    }
else:
    ABLIVE_SERVER_URL = ""
    MY_DB_CONFIG = {
        "host": "",
        "port": ,
        "user": "",
        "password": "",
    }
