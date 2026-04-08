import os
import sys
import requests
import getpass
import json
import argparse #this will be used to parse the CLI arguments
from typing import Optional

API_BASE_URL = "http://localhost:8000"
KEYCLOAK_URL = "http://localhost:8080/realms/MapReduce-Realm/protocol/openid-connect/token"
CLIENT_ID = "ui-service" 
TOKEN_FILE = ".frappe_tokens" # we will store the user's JWT issued from keycloak, in a local file

BANNER = r"""
                                                                               / /
                                                                              / /
                                                                             / /
                                                                      ______/ /_________
                                                                     /                  \
                                                                    |____________________|
 ___ ___   ____  ____   ____   ___   ___    __ __    __    ___       |                  |
|   |   | /    ||    \|    \  /  _] |   \  |  |  |  /  ]  /  _]      |                  |
| _   _ ||  o  ||  o  )  D  )/   [_ |     \|  |  | /  /  /  [_       |==================|
|  \_/  ||     ||   _/|    / |    _]|  D  ||  |  |/  /  |    _]      |@@@@@@@@@@@@@@@@@@|
|   |   ||  _  ||  |  |    \ |   [_ |     ||  :  /   \_ |   [_       |@@@@          @@@@|
|   |   ||  |  ||  |  |  .  \|     ||     ||     \     ||     |      |@@@            @@@|
|___|___||__|__||__|  |__|\_||_____||_____| \__,_|\____||_____|      |@@    FRAPPE    @@|
                                                                     |@@@            @@@|
                                                                     |@@@@          @@@@|
                                                                     |@@@@@@@@@@@@@@@@@@|
                                                                     |@@@@@@@@@@@@@@@@@@|
                                                                      \@@@@@@@@@@@@@@@@/
                                                                       `--------------`
"""
def print_banner():
    # \033[96m changes the color to cyan!
    print("\033[96m" + BANNER + "\033[0m")
    print("\033[1;32mWelcome to the distributed Frappe MapReduce Engine!\033[0m\n")

""" =================== TOKEN HANDLING ======================= """

def login_keycloak() -> Optional[str]:
    #prompt user for credentials, issue the JWT
    print("\n \033[1;32m======== Keycloak Login ========\033[0m")
    print("If you have not signed up via Keycloak yet, contact your admin. You can only log in here, via CLI.\n")
    username = input("\033[1;32mUsername: \033[0m ")
    password = getpass.getpass("\033[1;32mPassword: \033[0m ")

    payload = {
        'client_id': CLIENT_ID,
        'username': username,
        'password': password,
        'grant_type': 'password',
        'scope' : 'openid profile email'
    }

    try:
        response = requests.post(KEYCLOAK_URL, data=payload)
        response.raise_for_status() #this will raise exception if the response status is 4xx or 5xx (error)

        data = response.json() #this is the entire response, it will contain many fields, such as the token, expiration time, refresh token, etc
        # uncomment to see the entire resposne
        #print(data)
        
        access_token = data.get('access_token') # we care to extract the access token from the response
        refresh_token = data.get('refresh_token') # and the refresh token, but that's secondary for now

        if not access_token:
            print("\033[91mError: You were authenticated, but no token was issued by Keycloak. This shouldn't have happened. \033[0m")
            return None

        token_data = {
            "access_token": access_token,
            "refresh_token": refresh_token,
        }

        # save the tokens to file
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f)
        
        print(f"\033[1;32mLogin successful! Your token is saved in .frappe_tokens\033[0m")
        return access_token
    
    except requests.exceptions.RequestException as e:
        print(f"\033[91mError: {e}\033[0m")
        return None
    
def load_token() -> Optional[str]:
    """ Retrieve token if it exists locally (it will, if you have logged in before)"""
    if not os.path.exists(TOKEN_FILE):
        return None
    
    try:
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.load(f)
        return token_data.get('access_token')
    except (json.JSONDecodeError, IOError):
        return None

def get_headers():
    token = load_token()
    if not token:
        print("\033[91mError: No token found. Log in first.\033[0m")
        sys.exit(1)

    return {
        "Authorization": f"Bearer {token}" #this is what will be used to authenticate requests to the ui-service
    } 

""" =================== PARSE ARGUMENTS IN CLI FOR ENDPOINTS ======================= """

def upload_minio(input_path:str, mapper_path:str, reducer_path:str):
    """ Upload the 3 required files to minio """
    headers = get_headers()

    files = {
        "input_data": (os.path.basename(input_path), open(input_path, "rb")),
        "mapper_code": (os.path.basename(mapper_path), open(mapper_path, "rb")),
        "reducer_code": (os.path.basename(reducer_path), open(reducer_path, "rb")),
    }   

    try:
        print(f"\033[1;32mAttempting to upload files to MinIO...\033[0m")
        response = requests.post(f"{API_BASE_URL}/files/upload", headers=headers, files=files)

        response.raise_for_status()
        
        print(f"\033[1;32mSuccessfully uploaded files to MinIO\033[0m")
        return response.json() # this will return the refs for the uploaded files

    except requests.exceptions.RequestException as e:
        if response.status_code == 401:
            print("\033[91m[Error 401 - Unauthorized]. Please run 'python3 cli.py login' first.\033[0m")
        else:
            print(f"\033[91mUpload Failed: {e}\033[0m")
        return None
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Frappe MapReduce CLI")
    # We create "sub-commands"
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # -- login --
    subparsers.add_parser("login", help="Authenticate with Keycloak")

    # -- upload --
    up_parser = subparsers.add_parser("upload", help="Upload a new workspace")
    up_parser.add_argument("--input", required=True, help="Path to data file")
    up_parser.add_argument("--mapper", required=True, help="Path to mapper script")
    up_parser.add_argument("--reducer", required=True, help="Path to reducer script")

    args = parser.parse_args()

    # --- execution logic ---

    if not args.command: # if no command is provided, print the banner and the help message
        print_banner()
        parser.print_help()
        sys.exit(0)

    if args.command == "login": 
        print_banner()
        login_keycloak()
        sys.exit(0)
    
    # if the user tries to upload without being logged in, we should prompt them to log in
    if args.command == "upload":
        # we don't print the banner for every command, keep it clean
        upload_minio(args.input, args.mapper, args.reducer)
