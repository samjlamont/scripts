import shutil
import concurrent.futures

import pandas as pd


def delete_from_tmp(run_id):
    dir_path = f"/var/lib/oneconcern/tmp/{run_id}"
    try:
        shutil.rmtree(dir_path)
        print(f"{dir_path} successfully removed!")
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))


def delete_from_input(run_id):
    dir_path = f"/var/lib/oneconcern/input/{run_id}"
    try:
        shutil.rmtree(dir_path)
        print(f"{dir_path} successfully removed!")
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))


def delete_from_output(run_id):
    dir_path = f"/var/lib/oneconcern/output/{run_id}"
    try:
        shutil.rmtree(dir_path)
        print(f"{dir_path} successfully removed!")
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))


def delete_from_inundation_schism(run_id):
    dir_path = f"/var/lib/oneconcern/inundation_schism/{run_id}"
    try:
        shutil.rmtree(dir_path)
        print(f"{dir_path} successfully removed!")
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))


if __name__ == "__main__":

    # parser = argparse.ArgumentParser()
    # parser.add_argument("path_to_runid_file", type=str)
    # args = parser.parse_args()
    # print(f"Deleting run IDs listed in: {args.path_to_runid_file}")

    path_to_runid_file = "/var/lib/oneconcern/run_ids_to_delete.log"

    lst_runids = pd.read_csv(path_to_runid_file, header=None)[0].tolist()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(delete_from_tmp, lst_runids)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(delete_from_input, lst_runids)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(delete_from_output, lst_runids)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(delete_from_inundation_schism, lst_runids)