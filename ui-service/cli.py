import os
import sys
import requests
import getpass
from typing import Optional

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

        token_data = {
            "access_token": access_token,
            "refresh_token": refresh_token,
        }

        # save the tokens to file
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f)

        if not access_token:
            print("\033[91mError: You were authenticated, but no token was issued by Keycloak. This shouldn't have happened. \033[0m")
            return None
        
        # Save the token to a file
        with open(TOKEN_FILE, 'w') as f:
            f.write(access_token)
        
        print(f"\033[1;32mLogin successful! Your token is saved in .frappe_token\033[0m")
        return access_token
    
    except requests.exceptions.RequestException as e:
        print(f"\033[91mError: {e}\033[0m")
        return None
    
    
if __name__ == "__main__":
    print_banner()
    login_keycloak()
    sys.exit(0)
