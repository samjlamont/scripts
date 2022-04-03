from typing import Tuple, Optional
import os
import re
import time

import xarray as xr
import numpy as np
from netCDF4 import Dataset
import gzip

# Indices for schout_*.nc files
START_INDX = 144  # Allow for 6-day spin-up time
END_INDX = 240   # End of 10-day run
SCHISM_OUTPUT_FILES_REGEX = r'schout_[0-9]+\.nc\.gz'


def get_schism_coords(filepath: str) -> Tuple[np.array, np.array, np.array]:
    with xr.open_dataset(filepath) as ds_sch:
        x = ds_sch.SCHISM_hgrid_node_x.values.astype(np.float32)
        y = ds_sch.SCHISM_hgrid_node_y.values.astype(np.float32)
        faces = ds_sch.SCHISM_hgrid_face_nodes.data.astype(int) - 1
    return x, y, faces


def get_schism_water_depth(filepath: str, output_timestep="hourly") -> np.array:
    with xr.open_dataset(filepath) as ds_sch:
        arr_bathy = ds_sch.depth.values.astype(np.float32)
        if output_timestep == "hourly":
            arr_elev = ds_sch.elev.values.ravel().astype(np.float32)
        elif output_timestep == "daily":  # Is this even necessary?
            arr_elev = arr_elev = ds_sch.elev.max(axis=0).values.astype(np.float32)
        else:
            raise ValueError("Problem with output_timestep variable!")
        arr_depth = arr_elev + arr_bathy
        arr_depth[arr_depth < 0.0] = 0.0
        # # Mask out values based on the threshold
        # arr_depth[arr_depth <= DEPTH_THRESHOLD] = 0.0
    return arr_depth


def get_schism_max_water_depth(
    path_to_schism_output: str,
    start_index: int,
    end_index: int,
    output_timestep="hourly",
) -> Tuple[np.array, np.array, np.array, np.array]:
    for indx, i in enumerate(range(start_index, end_index + 1)):
        output_file = f"{path_to_schism_output}/schout_{i}.nc"
        if indx == 0:
            x, y, faces = get_schism_coords(output_file)  # node xy's
        depth = get_schism_water_depth(
            output_file, output_timestep
        )  # depth at each node
        if i == start_index:
            comp_depth = depth
        comp_depth = np.maximum(comp_depth, depth)

    return x, y, comp_depth, faces


def get_xyz_data(file_path: Optional[str], file_in_memory: Optional) -> Tuple:
    """
    Function to extract xyz (x-axis, y-axis and depth) data from schism output netCDF file. There will be one output
    file per 1 hour. If the flood model is run for 3 hour time period, then there will be 3 files
    """
    if file_path:
        dataset = Dataset(file_path, "r")  # read file from disk
    else:
        dataset = Dataset('schout_tmp_inmemory.nc', memory=file_in_memory)  # read file from bytes object in memory

    # bathymetry, positive down, meters
    bathy = dataset.variables["depth"][:].reshape(-1, 1)

    # water elevation at node
    elev = dataset.variables["elev"][:, :].reshape(-1, 1)

    # calculate water depth
    depth = elev + bathy

    # set non-positive depth as 0
    depth[depth < 0.00001] = 0

    x_axis = dataset.variables["SCHISM_hgrid_node_x"][:].reshape(-1, 1)
    y_axis = dataset.variables["SCHISM_hgrid_node_y"][:].reshape(-1, 1)
    faces = dataset.variables["SCHISM_hgrid_face_nodes"][:].astype(int) - 1

    dataset.close()
    return x_axis, y_axis, faces, depth


def get_schism_max_water_depth_gzip(grid_no: str, run_id: int) -> None:

    t1 = time.time()

    # Get list of files
    nfs_run_grid_files_location = f"/var/lib/oneconcern/inundation_schism/{run_id}/{grid_no}/outputs"
    output_files_list = [
        file_name for file_name in os.listdir(nfs_run_grid_files_location) if
        re.match(SCHISM_OUTPUT_FILES_REGEX, file_name)
    ]
    # Get max depth
    x_axis, y_axis, depth_max, faces = None, None, None, None
    for output_file in output_files_list:
        output_file_num = re.search('[0-9]+', output_file)
        if not output_file_num:
            continue  # the file doesn't have a number in the name, skip
        if (int(output_file_num.group()) < START_INDX) | (int(output_file_num.group()) > END_INDX):
            continue
            # print("TEMP!")
        with gzip.open(f"{nfs_run_grid_files_location}/{output_file}", 'rb') as f_in:
            if depth_max is None:
                x_axis, y_axis, faces, depth_max = get_xyz_data(file_path=None, file_in_memory=f_in.read())
            else:
                _, _, _, depth_current = get_xyz_data(file_path=None, file_in_memory=f_in.read())
                depth_max = np.maximum(depth_max, depth_current)

    df_tris = pd.DataFrame(faces[:, 0:3], columns=['v0', 'v1', 'v2'])
    df_max = pd.DataFrame({'node_x': x_axis.ravel(), 'node_y': y_axis.ravel(), f'max_depth': depth_max.ravel()})

    df_tris.to_parquet(f"{nfs_run_grid_files_location}/{run_id}_faces.parquet", index=False)
    df_max.to_parquet(f"{nfs_run_grid_files_location}/{run_id}_max_depth.parquet", index=False)

    print(f"\tDone with {run_id}! Total elapsed time: {(time.time() - t1) / 60:.2f} mins")

    return


if __name__ == "__main__":

    # Needed input
    # grid_no = "grid_1"
    # run_id = 12810327

    lst_runids = [12810327,
                  94746831,
                  58954209,
                  6986044,
                  36996533]

    for run_id in lst_runids:
        get_schism_max_water_depth_gzip("grid_1", run_id)

    print("done!")

