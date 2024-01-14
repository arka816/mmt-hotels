'''
    detect chrome version in a platform-independent manner
    Compatible with Windows, Mac and linux
'''
import os
import re
from sys import platform


def extract_version_folder():
    # check if the Chrome folder exists in the x32 or x64 Program Files directories
    program_folders = ['Program Files', 'Program Files (x86)']

    for program_folder in program_folders:
        folder = os.path.join("C:/", program_folder, "Google/Chrome/Application")

        if os.path.isdir(folder):
            for path in [f.path for f in os.scandir(path) if f.is_dir()]:
                filename = os.path.basename(path)
                pattern = '\d+\.\d+\.\d+\.\d+'
                match = re.search(pattern, filename)
                if match and match.group():
                    # Found a Chrome version.
                    return match.group(0)

    return None


def get_chrome_version():
    try:
        if platform == 'linux' or platform == 'linux2':
            version = os.popen("/usr/bin/google-chrome --version").read().lower().strip('google chrome').strip()
        elif platform == 'darwin':
            version = os.popen("/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --version").read().lower().strip('google chrome').strip()
        elif platform == 'win32':
            try:
                # try registry key
                key = 'HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon'
                output = os.popen(f'reg query "{key}" /v version').read()
                version = output.strip().strip("\n").strip().split()[-1]
            except:
                # try searching for folder
                version = extract_version_folder()
            return version
    except:
        return None
