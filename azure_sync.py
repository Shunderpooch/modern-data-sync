# Author Arthur Dooner
# Project Start Date: 06/20/18
# Last Modified 06/28/18
import calendar
import datetime
import json
import os
import time
import yaml

from azure.datalake.store import core, lib, multithread

adls_name = 'ENTER_NAME_HERE'

def local_path_tree(local_path):
    directories = [d for d in os.listdir(local_path) if os.path.isdir(os.path.join(local_path, d))]
    outputs = [{"name": os.path.join(local_path, d), "modificationTime": os.path.getmtime(os.path.join(local_path, d)), "type": "DIRECTORY"} for d in directories]
    # If there are directories then we should check them too
    if len(directories):
        for directory in outputs:
            directory["contents"] = local_path_list(os.path.join(directory["name"]))
    files = [a_file for a_file in os.listdir(local_path) if os.path.isfile(os.path.join(local_path, a_file))]
    outputs.extend([{"name": os.path.join(local_path, a_file), "modificationTime": os.path.getmtime(os.path.join(local_path, a_file)), "length": os.path.getsize(os.path.join(local_path, a_file)), "type":"FILE"} for a_file in files])
    return outputs

def local_path_tree_with_metadata(local_path, date_accessed=None):
    local_outputs = local_path_tree(local_path)
    return {"dateAccessed": date_accessed if date_accessed else calendar.timegm(time.gmtime()), "files":local_outputs}

def adls_path_tree(adls_path):
    outputs = adl.ls(adls_path, detail=True)
    directories = [directory for directory in outputs if directory['type'] == 'DIRECTORY']
    if len(directories):
        for directory in directories:
            directory["contents"] = adls_path_tree(directory["name"])
    return outputs

def adls_path_tree_with_metadata(adls_path, date_accessed=None):
    adls_outputs = adls_path_tree(adls_path)
    return {"dateAccessed": date_accessed if date_accessed else calendar.timegm(time.gmtime()), "files":adls_outputs}

def get_tree(folder, action, name_offset=0):
    results = []
    for element in folder["contents"]:
        if element["type"] == 'DIRECTORY':
            results.extend(get_tree(element, action, name_offset))
        else:
            results.append({"name":element["name"][name_offset:], "type":element["type"], "action": action})
    return results

def file_comparator(local_file, remote_file):
    if local_file["modificationTime"] > remote_file["modificationTime"]/1000:
        return {
            "name": remote_file["name"],
            "type": remote_file["type"],
            "action": "UPLOAD SINCE LOCAL CHANGES"
        }
    else:
        return {
            "name": remote_file["name"],
            "type": remote_file["type"],
            "action": "DOWNLOAD SINCE REMOTE CHANGES"
        }

def folder_comparator(local_folder, remote_folder):
    diff_list = []
    local_tree = [x["name"] for x in local_folder]
    remote_tree = ["./" + x["name"] for x in remote_folder]
    local_only_elements = list(set(local_tree).difference(remote_tree))
    for remote_element in remote_tree:
        local_element = next((element for element in local_tree if remote_element["name"] in element["name"]), None)
        if (local_element):
            if remote_element["type"] == "FILE":
                diff_list.append(file_comparator(local_element, remote_element))
            elif remote_element["type"] == "DIRECTORY":
                diff_list.extend(folder_comparator(local_element["contents"], remote_element["contents"]))
            else:
                print("Something went wrong!")
        else: # Local copies of the remote files do not exist
            if remote_element["type"] == "FILE":
                diff_list.append({
                    "name": remote_element["name"],
                    "type": remote_element["type"],
                    "action": "DOWNLOAD"
                })
            elif remote_element["type"] == "DIRECTORY":
                diff_list.extend(get_tree(remote_element, 'DOWNLOAD', 0))
            else:
                print("Something went wrong!")
    for local_name in local_only_elements:
        local_element = next((x for x in local_tree if local_name in x["name"]), None)
        if local_element:
            if local_element["type"] == "FILE":
                diff_list.append({
                    "name": local_element["name"][2:],
                    "type": local_element["type"],
                    "action": 'UPLOAD'
                })
            elif local_element["type"] == "DIRECTORY":
                diff_list.extend(get_tree(local_element, "UPLOAD", 2))
    return diff_list

def get_diff_list(local_folder, remote_folder, adl, go_to_adls=True):
    # Acquire the local folder directories
    os.chdir(local_folder) # The location we'll be working out of for the remainder of the operation
    local_tree = local_path_tree_with_metadata(".") # Can't be parameterless yet
    adls_tree = {}
    # Based on whether or not we want to go to Azure and get the state or use a saved state to reduce the number of hits we make to the DLS
    if (go_to_adls):
        adls_tree = adls_path_tree_with_metadata(remote_folder)
        with open('adls-state.json', 'w') as file_to_cache:
            json.dump(adls_tree, file_to_cache, indent=4) # Make this human readable for the time being
    else: # Read from our cached file
        with open('adls-state.json', 'r') as cached_file:
            adls_tree = json.load(cached_file)
        # TODO: Add a section to check that this actually worked
    # Now we have the ADLS Tree and the local tree
    diff_list = folder_comparator(local_tree["files"], adls_tree["files"])
    return diff_list

def print_stats_and_warning(diff_list, save_file=None):
    if save_file:
        with open(save_file, "w") as outfile:
            json.dump(diff_list, outfile, indent=4)
    print("The following elements will be uploaded to your Azure Data Lake Store:")
    for element in sorted([element for element in diff_list if element["action"] == "UPLOAD"], key=lambda x: str(x["type"] + x["name"])):
        print(f'⇧ {"{:10s}".format(element["type"])} {element["name"]}')
    print("")
    print("The following elements will be downloaded from your Azure Data Lake Store:")
    for element in sorted([element for element in diff_list if element["action"] == "DOWNLOAD"], key=lambda x: str(x["type"] + x["name"])):
        print(f'⇩ {"{:10s}".format(element["type"])} {element["name"]}')
    print("")
    print("The following elements will be uploaded from your computer, overwriting the existing files in your Azure Directory")
    for element in sorted([element for element in diff_list if element["action"] == "UPLOAD SINCE LOCAL CHANGES"], key=lambda x: str(x["type"] + x["name"])):
        print(f'⇧ OW {"{:10s}".format(element["type"])} {element["name"]}')
    print("")
    print("The following elements will be downloaded from your Azure Data Lake Store, overwriting the existing files on your computer")
    for element in sorted([element for element in diff_list if element["action"] == "DOWNLOAD SINCE REMOTE CHANGES"], key=lambda x: str(x["type"] + x["name"])):
        print(f'⇩ OW {"{:10s}".format(element["type"])} {element["name"]}')
    print("")
    upload_files_len = len([files for files in diff_list if "UPLOAD" in files["action"]])
    print(f'{str(upload_files_len)} files will be uploaded and {len(diff_list) - upload_files_len} files will be downloaded.')
    push = input(f'Are you sure you want these {len(diff_list)} files to be changed? ')
    if push.lower() != "yes" or push.lower() != "y":
        exit()

def upload_download(adl, diff_list):
    for element in sorted([element for element in diff_list if "UPLOAD" in element["action"]], key=lambda x: str(x["type"] + x["name"])):
        print(element["name"])
        multithread.ADLUploader(adl, rpath=element["name"], lpath="./" + element["name"], nthreads=64, overwrite=True, buffersize=4194034, blocksize=4194304)
    for element in sorted([element for element in diff_list if "DOWNLOAD" in element["action"]], key=lambda x: str(x["type"] + x["name"])):
        print(element["name"])
        multithread.ADLDownloader(adl, rpath=element["name"], lpath="./" + element["name"], nthreads=64, overwrite=True, buffersize=4194034, blocksize=4194304)

adls_configs = None

#Setup

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'adls_config.yml'), 'r') as config_file:
    adls_configs = yaml.load(config_file)

adls_creds = lib.auth(tenant_id=adls_configs["tenant_id"], client_secret=adls_configs["client_secret"], client_id=adls_configs["client_id"])
adl = core.AzureDLFileSystem(adls_creds, store_name=adls_configs["adls_name"])

# Actual runtime

diff_list = get_diff_list(adls_configs["local_folder"], adls_configs["adls_folder"], adl, go_to_adls=True)
print_stats_and_warning(diff_list, save_file='action-list.json')
upload_download(adl, diff_list)

# Need to persist the state of the directories
with open('preexisting-adls-state.json', 'w') as outfile:
    json.dump(adls_path_tree_with_metadata(adls_configs["adls_folder"]), outfile, indent=4)
with open('preexisting-local-state.json', 'w') as outfile:
    json.dump(local_path_tree_with_metadata(adls_configs["local_folder"]), outfile, indent=4)
    
print("Script complete!")
