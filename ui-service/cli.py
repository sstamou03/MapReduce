import os
import sys
import requests
import getpass
import json
import argparse #this will be used to parse the CLI arguments
import datetime
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

""" =================== USER ENDPOINTS VIA CLI ======================= """

# -- file upload : POST /files/upload
def upload_minio(input_path:str, mapper_path:str, reducer_path:str):
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
        
        print(f"\033[1;32mSuccessfully uploaded files to MinIO!\nYour files are stored in MinIO under the following references:\033[0m")
        print(f"Input reference: {response.json().get('input_ref')}")
        print(f"Mapper reference: {response.json().get('mapper_ref')}")
        print(f"Reducer reference: {response.json().get('reducer_ref')}")
        return response.json() # this will return the refs for the uploaded files

    except requests.exceptions.RequestException as e:
        if response.status_code == 401:
            print("\033[91m[Error 401 - Unauthorized]. Please run 'python3 cli.py login' first.\033[0m")
        else:
            print(f"\033[91mUpload Failed: {e}\033[0m")
        return None
    
# -- get user jobs : GET /jobs
def get_jobs():
    headers = get_headers()
    try:
        print(f"\033[1;32mAttempting to retrieve your jobs from the database...\033[0m")
        response = requests.get(f"{API_BASE_URL}/jobs", headers=headers)

        response.raise_for_status()
        
        print(f"\033[1;32mSuccessfully retrieved jobs\033[0m")
        
        # we will print only the job IDs, status and created at
        for job in response.json():
            #format time to be more readable
            created_at = job['created_at']
            formatted_time = datetime.datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")
            print(f"Job ID: {job['job_id']}\tStatus: {job['status']}\tCreated At: {formatted_time}")
        
        return response.json() # this will return the refs for the uploaded files

    except requests.exceptions.RequestException as e:
        if response.status_code == 401:
            print("\033[91m[Error 401 - Unauthorized]. Please run 'python3 cli.py login' first.\033[0m")
        else:
            print(f"\033[91mFailed to fetch jobs: {e}\033[0m")
        return None

# -- submit job : POST /jobs
def submit_job(input_ref:str, mapper_ref:str, reducer_ref:str):
    headers = get_headers()
    payload = {
        "input_code_ref": input_ref,
        "mapper_code_ref": mapper_ref,
        "reducer_code_ref": reducer_ref,
    }
    try:
        print(f"\033[1;32mAttempting to submit job to the database...\033[0m")
        response = requests.post(f"{API_BASE_URL}/jobs", headers=headers, json=payload)

        response.raise_for_status()
        
        print(f"\033[1;32mSuccessfully submitted job\033[0m")
        print(response.json())
        return response.json() # this will return the job details

    except requests.exceptions.RequestException as e:
        if response.status_code == 401:
            print("\033[91m[Error 401 - Unauthorized]. Please run 'python3 cli.py login' first.\033[0m")
        else:
            print(f"\033[91mFailed to submit job: {e}\033[0m")
        return None
        
""" =================== ADMIN ENDPOINTS VIA CLI ======================= """
def get_all_jobs():
    headers = get_headers()
    try:
        print(f"\033[1;32mAttempting to retrieve all jobs from the database...\033[0m")
        response = requests.get(f"{API_BASE_URL}/admin/jobs", headers=headers)

        response.raise_for_status()
        
        print(f"\033[1;32mSuccessfully retrieved all jobs\033[0m")
        
        # we will print only the job IDs, status and created at
        for job in response.json():
            #format time to be more readable
            created_at = job['created_at']
            formatted_time = datetime.datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")
            print(f"User: {job['user_id']}\tJob ID: {job['job_id']}\tStatus: {job['status']}\tCreated At: {formatted_time}")
        
        return response.json() # this will return the refs for the uploaded files

    except requests.exceptions.RequestException as e:
        if response.status_code == 401:
            print("\033[91m[Error 401 - Unauthorized]. Please run 'python3 cli.py login' first.\033[0m")
        elif response.status_code == 403:
            print("\033[91m[Error 403 - Forbidden]. You're not an admin, don't try access admin endpoints.\033[0m")
        else:
            print(f"\033[91mFailed to fetch jobs: {e}\033[0m")
        return None
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Frappe MapReduce CLI")

    # this tells argparse to expect a specific keyword after 'python3 cli.py' (e.g. 'login' or 'upload')
    # dest="command" means that the value of the keyword will be stored in the 'args.command' variable
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    ''' ============ USER & ADMIN ENDPOINTS ============'''
    # -- login --
    subparsers.add_parser("login", help="Authenticate with Keycloak")

    # -- upload --
    up_parser = subparsers.add_parser("upload", help="Upload a new workspace")
    up_parser.add_argument("--input", required=True, help="Path to data file")
    up_parser.add_argument("--mapper", required=True, help="Path to mapper script")
    up_parser.add_argument("--reducer", required=True, help="Path to reducer script")

    # -- get user jobs --
    subparsers.add_parser("jobs", help="Get all jobs for the authenticated user")

    # -- submit job --
    submit_parser = subparsers.add_parser("submit", help="Submit a new job")
    submit_parser.add_argument("--input_ref", required=True, help="Reference to the input data")
    submit_parser.add_argument("--mapper_ref", required=True, help="Reference to the mapper code")
    submit_parser.add_argument("--reducer_ref", required=True, help="Reference to the reducer code")
    
    ''' ============ ADMIN ENDPOINTS ============'''
    # -- get all jobs --
    subparsers.add_parser("all-jobs", help="Get all jobs for the authenticated user")

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
    
    # if the user tries to upload without being logged in, they will get a 401 error - unauthorized
    if args.command == "upload":
        # we don't print the banner for every command, keep it clean
        upload_minio(args.input, args.mapper, args.reducer)
        sys.exit(0)
    
    if args.command == "submit":
        submit_job(args.input_ref, args.mapper_ref, args.reducer_ref)
        sys.exit(0)
    
    if args.command == "jobs":
        get_jobs()
        sys.exit(0)
    
    if args.command == "all-jobs":
        get_all_jobs()
        sys.exit(0)
