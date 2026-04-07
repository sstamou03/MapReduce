import os
import sys

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
|___|___||__|__||__|  |__|\_||_____||_____| \__,_|\____||_____|      |@@    FREDDO    @@|
                                                                     |@@@  ESPRESSO  @@@|
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

if __name__ == "__main__":
    # Ensure Windows terminals support ANSI colors
    os.system('color') 
    print_banner()
    sys.exit(0)
