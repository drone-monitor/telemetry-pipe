import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytz
from ardupilot_log_reader.reader import Ardupilot


# LOGS_DIR ='/media/mh/balmas 18/18.10 KARISH NOAH 1/APM/LOGS/'

DRONE_SPEED = 19  # meters per second
TIME_INTERVAL = 1  # time interval between images in seconds
logger = logging.getLogger(__name__)


def unix_time_to_israel_datetime(unix_time: int) -> str:
    tz_israel = pytz.timezone("Israel")
    utc_time = datetime.utcfromtimestamp(unix_time).replace(tzinfo=pytz.utc)
    israel_time = utc_time.astimezone(tz_israel)
    return israel_time.strftime("%Y:%m:%d %H:%M:%S")


class DroneData:
    """
    Initialize the Drone_data class.

    Args:
        images_input_df (pd.DataFrame): DataFrame containing image data.
        logs__dir_ (str): Path to the directory containing log files (BINs).
    This class is used for processing drone data and logs.
    """

    def __init__(
        self, images_input_df: pd.DataFrame, logs_dir_: str, get_loc: bool = False
    ):
        self.images_input_df = images_input_df
        self.logs__dir = logs_dir_
        self.images_pipe_df = self.images_input_df.copy()
        self.all_log_dfs_BARO_ = pd.DataFrame()
        self.all_log_dfs_ATT_ = pd.DataFrame()
        self.all_log_dfs_GPS_ = pd.DataFrame()
        self.all_log_dfs_RCIN_ = pd.DataFrame()

        self.get_obox_gps = get_loc
        self.time_delta = 0

    def assign_closest_bar_alt(self):
        """
        Assign the closest barometric altitude to images.

        This function merges barometric altitude data with image data.
        """
        # test if merge is on same dtype if not raise error
        if (
            self.images_pipe_df["unix_time_created"].dtype
            != self.all_log_dfs_BARO_["timestamp"].dtype
        ):
            logger.error("Error in unix_time_created dtype / logs_BARO timestamp")
            raise Exception("Error in unix_time_created dtype / logs_BARO timestamp")

        self.images_pipe_df = pd.merge_asof(
            self.images_pipe_df.sort_values("unix_time_created"),
            self.all_log_dfs_BARO_[["timestamp", "BAROAlt"]].sort_values("timestamp"),
            left_on="unix_time_created",
            right_on="timestamp",
            direction="nearest",
        )
        self.images_pipe_df.drop("timestamp", axis=1, inplace=True)

    def assign_closest_ATT_roll(self):
        """
        Assign the closest roll data to images.

        This function merges roll data with image data.
        """
        if (
            self.images_pipe_df["unix_time_created"].dtype
            != self.all_log_dfs_ATT_["timestamp"].dtype
        ):
            logger.error("Error in unix_time_created dtype / logs_ATT timestamp")
            raise Exception("Error in unix_time_created dtype / logs_ATT timestamp")

        self.images_pipe_df = pd.merge_asof(
            self.images_pipe_df.sort_values("unix_time_created"),
            self.all_log_dfs_ATT_[["timestamp", "ATTRoll"]].sort_values("timestamp"),
            left_on="unix_time_created",
            right_on="timestamp",
            direction="nearest",
        )
        self.images_pipe_df.drop("timestamp", axis=1, inplace=True)

    def get_offset_n_align_time(self):
        # find the drop in RCINC10 and align the time of the images_input_df accuretly only if the time delta is big enough
        # find the drop

        rcin_name = "RCINC10"
        rcinc_drop = self.all_log_dfs_RCIN_[rcin_name].diff().idxmin()
        # find the longest time period between diff()
        drops = self.all_log_dfs_RCIN_[rcin_name].diff().loc[lambda x: x < 0]
        climbs = self.all_log_dfs_RCIN_[rcin_name].diff().loc[lambda x: x > 0]

        if len(drops) > 1:
            drop_positions = np.array(drops.index)
            climbs_positions = np.array(climbs.index)
            # align size to the smallest
            if len(drop_positions) > len(climbs_positions):
                drop_positions = drop_positions[: len(climbs_positions)]
            elif len(drop_positions) < len(climbs_positions):
                climbs_positions = climbs_positions[: len(drop_positions)]

            # calculate the distances for each drop position and its nearest climb position
            distances = np.abs(drop_positions - climbs_positions)
            # find the drop postion with the biggest distans to its corresponding climb
            rcinc_drop = drop_positions[distances.argmax()]

        time_trigger = self.all_log_dfs_RCIN_.loc[rcinc_drop]["timestamp"]
        # find the time delta
        self.time_delta = time_trigger - self.images_pipe_df["unix_time_created"].min()
        if (
            abs(self.time_delta) > timedelta(days=1).total_seconds()
            and abs(self.time_delta) < timedelta(days=10).total_seconds()
        ):
            logger.error("Error in time delta between images and logs")
            raise Exception("Error in time delta between images and logs")
        # align the time
        elif abs(self.time_delta) > timedelta(days=10).total_seconds():
            self.time_delta = 0
            logger.error("cannot align time delta is too big erro in logs BIN")

            log_max_time = self.all_log_dfs_RCIN_.iloc[-1]["timestamp"]
            days, remainder = divmod(log_max_time, 3600 * 24)
            hours, remainder = divmod(remainder, 3600)
            minutes, seconds = divmod(remainder, 60)
            logger.error(
                f"total flight duration: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
            )
            if days > 1:
                logger.error("flight duration is more than 1 day, please check logs")
            else:
                logger.info(
                    "flight duration is less than 1 day, aligning time backwards based on images"
                )
                self.time_bias = (
                    self.images_pipe_df["unix_time_created"].min() - time_trigger
                )
                self.all_log_dfs_RCIN_["timestamp"] = (
                    self.all_log_dfs_RCIN_["timestamp"] + self.time_bias
                )
                self.all_log_dfs_ATT_["timestamp"] = (
                    self.all_log_dfs_ATT_["timestamp"] + self.time_bias
                )
                self.all_log_dfs_BARO_["timestamp"] = (
                    self.all_log_dfs_BARO_["timestamp"] + self.time_bias
                )
                self.all_log_dfs_GPS_["timestamp"] = (
                    self.all_log_dfs_GPS_["timestamp"] + self.time_bias
                )
                logger.info("Done aligning time backwards based on images")

        self.images_pipe_df["unix_time_created"] = (
            self.images_pipe_df["unix_time_created"] + self.time_delta
        )

    def assign_obox_gps(self):
        """
        Assign the closest gps 1 (obox) data to images.
        This function assign gps , loc lat lon
        """
        if (
            self.images_pipe_df["unix_time_created"].dtype
            != self.all_log_dfs_GPS_["timestamp"].dtype
        ):
            logger.error("Error in unix_time_created dtype / logs_GPS timestamp")
            raise Exception("Error in unix_time_created dtype / logs_GPS timestamp")

        obox_gps_log = self.all_log_dfs_GPS_.copy()
        # if GPSU containg 1 -> obox if not rais error
        if 1 in obox_gps_log.GPSU.unique():
            obox_gps_log = obox_gps_log[obox_gps_log["GPSU"] == 1]
            self.images_pipe_df = pd.merge_asof(
                self.images_pipe_df.sort_values("unix_time_created"),
                obox_gps_log[["timestamp", "GPSLat", "GPSLng"]].sort_values(
                    "timestamp"
                ),
                left_on="unix_time_created",
                right_on="timestamp",
                direction="nearest",
            )
            self.images_pipe_df.drop("timestamp", axis=1, inplace=True)
            self.images_pipe_df.rename(
                columns={"GPSLat": "lat", "GPSLng": "lon"}, inplace=True
            )
        else:
            logger.error("did not find obox GPS in log")
            raise Exception("did not find obox GPS in log")

    def grab_data_from_log(self) -> tuple[pd.DataFrame, int]:
        """
        Retrieve altitude and roll data from log files and merge it with image data.

        Returns:
            pd.DataFrame: Merged DataFrame with altitude and roll data.
        """

        logger.info("Started to grab info from drone logs")

        logs__dir = str(Path(self.logs__dir).resolve())
        logger.info(f"Log directory: {logs__dir}")

        files = [f for f in os.listdir(logs__dir) if f.endswith(".BIN")]
        logger.info(f"Log files: {files}")

        # read all files with Ardupilot class and concat them
        for file in files:
            logger.info(f"reading BIN file:{file}")
            parser = Ardupilot(
                os.path.join(logs__dir, file),
                types=["ATT", "BARO", "GPS", "RCIN"],
                zero_time_base=False,
            )  # the log file, .bin
            logger.info(f"Finished reading BIN file:{file}, merging dfs...")
            self.all_log_dfs_BARO_ = pd.concat(
                [self.all_log_dfs_BARO_, parser.dfs["BARO"]], axis=0
            )
            self.all_log_dfs_ATT_ = pd.concat(
                [self.all_log_dfs_ATT_, parser.dfs["ATT"]], axis=0
            )
            self.all_log_dfs_GPS_ = pd.concat(
                [self.all_log_dfs_GPS_, parser.dfs["GPS"]], axis=0
            )
            self.all_log_dfs_RCIN_ = pd.concat(
                [self.all_log_dfs_RCIN_, parser.dfs["RCIN"]], axis=0
            )
            logger.info("Done merging")

        self.all_log_dfs_BARO_["timestamp"] = self.all_log_dfs_BARO_[
            "timestamp"
        ].astype("int64")
        self.all_log_dfs_ATT_["timestamp"] = self.all_log_dfs_ATT_["timestamp"].astype(
            "int64"
        )
        self.all_log_dfs_GPS_["timestamp"] = self.all_log_dfs_GPS_["timestamp"].astype(
            "int64"
        )
        self.all_log_dfs_RCIN_["timestamp"] = self.all_log_dfs_RCIN_[
            "timestamp"
        ].astype("int64")

        logger.info("get offset n align")
        self.get_offset_n_align_time()
        logger.info("assigning closest bar alt")
        self.assign_closest_bar_alt()
        logger.info("assigning closest ATT roll")
        self.assign_closest_ATT_roll()
        if self.get_obox_gps:
            logger.info("grabing gps locations from obox GPS..")
            self.assign_obox_gps()
            logger.info("DONE.")

        self.images_pipe_df.rename(
            columns={"BAROAlt": "altitude_ground", "ATTRoll": "roll"}, inplace=True
        )
        logger.info("Finished to grab info from drone logs")

        return self.images_pipe_df, abs(self.time_delta)
