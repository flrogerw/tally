#!/usr/bin/env python
"""The Zip.py module contains functions for exporting results for demographic analysis.

Arguments:
    - `-t` / `--type`: Required. Type of report to process. (ppm, diary)
    - `-f` / `--file-format`: Optional. File output format: csv|tsv (default: csv)
    - `-p` / `--period`: Optional. Numeric month or holiday, summer, spring, fall, winter. (default: current month)
    - `-y` / `--year`: Optional. Year of the report as a four-digit integer (default: current year).
    - `-m` / `--market`: Optional. Market ID for specific reporting.(default: all markets).
    - `--skip-s3`: Skips sending files to S3 bucket if present on the CLI. (default: False)

Envars:
    AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXX
    AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXX
    TALLY_SERVER = XXXXXXXXXXXXXXX  # URL for the remote Tally server.
    BUCKET_NAME=ent-adv-audio  # AWS S3 Bucket name.
    S3_PREFIX=nielsen_tally  # Do not include trailing slash.
    MAX_THREADS = 12  # Number of threads to start.
    DATA_DIR = "/tmp/tally/data"  # Temp dir for writing report files.
    IS_LOCAL = "False"  # Whether to run locally of use a remote server.

Functions:
    - get_all_but_zip_estimates: Retrieve and format audience estimates for a subset of ZIP codes.
    - get_audience_metrics: Calculate and compile final audience metrics and station demographic information.
    - get_day_parts: Generate a dictionary defining day part ranges for audience estimates.
    - get_dataset_list: Retrieve a list of loadable datasets for a specific month and year.
    - get_demographics: Construct a dictionary of demographic information based on the provided parameters.
    - get_end_results: Process zip code and station demographics to generate final estimates for market data.
    - get_est_request: Construct an estimate request for audience data.
    - get_filter_set: Create a filter set for demographic queries based on age, gender, and location.
    - get_filters: Create a set of demographic and geographic filters for audience estimation.
    - get_final_results: Process a completed queue and a list of datasets to generate a tabulation of results.
    - get_report_file_path: Return the file path for the report.
    - get_station_info: Retrieve and organize information about stations associated with a given dataset.
    - get_tot_market_estimates: Retrieve and structure total market estimates for each station.
    - get_zipcode_tuples: Generate zip code tuples and their corresponding 'all but' zipcodes for the dataset.
    - get_parsed_arguments: Parse command-line arguments for the report generation script.
    - process_demographic: Process a single demographic from the dataset.
    - process_task: Execute the full data processing workflow for a given dataset.
    - run_queries: Execute a series of queries to retrieve demographic estimates based on specified filters.
    - send_to_s3: Send files to an S3 bucket.
    - set_dataset: Load a specified dataset into the client's environment.
    - unload_all: Unload all datasets currently loaded in the specified client.
    - unload_dataset: Unload a specified dataset from the client.
    - validate_cli_args: Validate command-line arguments and their combinations.
    - validate_env: Validate the presence of required environment variables and raise an error if any are missing.
    - worker: Continuously retrieve and process tasks from the task_queue until it is empty.
    - write_to_file: Write demographic data to a CSV file based on the provided dataset and results.

"""
from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import product
from pathlib import Path
from queue import Empty, Full, Queue
from time import perf_counter as p_counter

import boto3
import requests
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError, PartialCredentialsError
from dateutil import tz
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zeep import Client, Settings, exceptions
from zeep.transports import Transport

# Load environment variables from .env file
load_dotenv()

# Access ENVARS from the environment.
DATA_DIR = os.getenv("DATA_DIR")
MAX_THREADS = os.getenv("MAX_THREADS")
IS_LOCAL = os.getenv("IS_LOCAL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")
TALLY_SERVER = os.getenv("TALLY_SERVER")

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TallyClient:
    """Singleton class to manage a single instance of a Tally SOAP client with custom retry strategies.

    Usage:
        - To access the Tally client, instantiate TallyClient using the `is_local` flag and `tally_server` URL
          as needed. The same instance will be returned on subsequent calls, ensuring only one client instance
          is created.

    """

    # Holds the single instance
    _instance = None

    def __new__(cls: type[TallyClient], is_local: bool, tally_server: str) -> TallyClient:  # noqa: PYI034
        """Ensure a single instance of TallyClient (singleton pattern).

        This method overrides the standard `__new__` constructor to check if an instance
        of TallyClient already exists. If not, it creates and initializes one. Otherwise,
        it returns the existing instance, ensuring that only one instance of TallyClient
        is created throughout the application lifecycle.

        Args:
            is_local (bool): If True, sets the WSDL endpoint to a local URL for testing.
            tally_server (str): URL of the Tally server WSDL, used when `is_local` is False.

        Returns:
            TallyClient: The single instance of TallyClient.

        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(is_local, tally_server)  # noqa: SLF001
        return cls._instance

    def _initialize(self, is_local: bool, tally_server: str) -> None:
        """Initialize the TallyClient instance with the SOAP client and session configuration.

        This method sets up a SOAP client using Zeep, configuring connection settings,
        retry strategy, and transport session for handling HTTP requests to the specified
        WSDL (either local or remote).

        Args:
            is_local (bool): If True, the WSDL endpoint is set to a local test URL.
            tally_server (str): The URL for the Tally server's WSDL, used if `is_local` is False.

        Returns:
            None

        """
        try:
            wsdl = "http://localhost:8181/ArbitronTallyWebServices/tally?wsdl" if is_local else tally_server

            headers_arr = {}
            retry_strategy = Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            )
            session = requests.Session()
            adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            transport = Transport(session=session)
            settings = Settings(strict=False, xml_huge_tree=True, extra_http_headers=headers_arr)

            self.t_client = Client(wsdl=wsdl, transport=transport, settings=settings)

        except exceptions.Fault:
            logging.exception("SOAP fault error.")
        except exceptions.TransportError:
            logging.exception("Transport error.")
        except exceptions.XMLSyntaxError:
            logging.exception("XML parsing error.")
        except requests.exceptions.ConnectionError:
            logging.exception("Failed to connect to the server.")
        except requests.exceptions.Timeout:
            logging.exception("Request timed out.")
        except Exception:
            logging.exception("An unexpected error occurred.")

    def get_service(self) -> Client.service:
        """Retrieve the SOAP service client configured in the TallyClient instance.

        This method returns the SOAP service client (`ServiceProxy`) that allows
        interaction with the Tally Web Services. The service client is initialized
        when the TallyClient is created and configured.

        Returns:
            ServiceProxy: An instance of the service client.

        """
        return self.t_client.service


class MissingEnvironmentVariableError(ValueError):
    """Custom exception raised when a required environment variable is not set.

    This exception is intended to be raised when a necessary environment variable
    is missing or not defined in the system's environment. It helps identify and
    notify when the application's configuration relies on specific variables that
    have not been initialized.

    Args:
        var_name (str): The name of the environment variable that is required
                        but not set.

    """

    def __init__(self, var_name: str) -> None:
        """Initialize the exception with the name of the missing environment variable."""
        super().__init__(f"{var_name} environment variable(s) is/are not set.")


def get_dataset_list(file_id: re.Pattern) -> list:
    """Retrieve a list of loadable datasets for a specific month and year.

    Args:
        file_id (str): Identifier used to search results and return pertinent datasets.

    Returns:
        A list of datasets that can be loaded, filtered by the specified month and year.

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        # Retrieve all loadable datasets from the client
        datasets_list = service.getLoadableDataSets()["reportingDataSets"]

        # Filter datasets to include only those matching the file identifier and return
        if args.rpt_type == 'ppm':
            datasets_to_process = [ds for ds in datasets_list if file_id in ds["id"]]

        elif args.rpt_type == 'diary':
            datasets_to_process = [ds for ds in datasets_list if
                                   file_id.match(ds["id"]) and str(args.rpt_year) in ds["fullSurveyName"]]

        else:
            datasets_to_process = []

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except exceptions.TransportError:
        logging.exception("Transport error.")
    except exceptions.XMLSyntaxError:
        logging.exception("XML parsing error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")
    else:
        return datasets_to_process


def get_station_info(ds: dict) -> dict:
    """Retrieve and organize information about stations associated with a given dataset.

    Args:
        ds (dict): A dictionary containing information about the dataset for which station data is requested.

    Returns:
        A dictionary mapping each station's ID to its corresponding details.

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        # Organize the station data into a dictionary for easy access
        return {
            stat["stationId"]: {
                "name": stat["name"],
                "callLetters": stat["callLetters"],
                "band": stat["band"],
            } for stat in service.getStations(ds)["stationsInList"]}

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except exceptions.ValidationError:
        logging.exception("Validation error.")
    except exceptions.TransportError:
        logging.exception("Transport error.")
    except exceptions.XMLSyntaxError:
        logging.exception("XML parsing error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except TypeError:
        logging.exception("Type error.")
    except ValueError:
        logging.exception("Value error.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def get_tot_market_estimates(ds: dict, filter_set: dict, day_parts: dict, metrics: list) -> dict:
    """Retrieve and structure total market estimates for each station.

    This function sends a request to retrieve total market estimates for a specified
    demographic and daypart configuration and organizes the response data by station.
    Each station's estimates are mapped by their estimate type codes (e.g., 'cume', 'aqh').

    Args:
        ds (dict): Dictionary containing dataset metadata, such as the dataset ID.
        filter_set (dict): Dictionary of filter criteria (e.g., demographics) for the request.
        day_parts (dict): Dictionary specifying start and end times for day parts.
        metrics (list): List of data points for the report.

    Returns:
        dict: A dictionary where each key is a station ID and each value is another dictionary
              mapping estimate type codes (such as 'cume' and 'aqh') to their respective values.

    """
    # Organize estimates for each station
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        # Build the estimate request with filtering
        tot_est_request = get_est_request(ds, filter_set, day_parts, metrics)
        return {
            market_estimates["stationId"]: {
                te["estimateTypeCode"]: te["value"]
                for te in market_estimates["estimates"]
            } for market_estimates in service.getEstimate(tot_est_request)["stationEstimates"]}

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def get_filters(start_age: int, end_age: int | str, gender: str, zips: str) -> list:
    """Create a set of demographic and geographic filters for audience estimation.

    This function generates a list of filters to be used in querying audience data. The filters include
    attributes such as starting age, ending age, sex, and ZIP codes. Conditions are applied based on
    input values: if `end_age` is not "+", a filter for the end age is added, and if `sex` is not "P"
    (presumably for "persons" or "all genders"), a filter for the specified sex is included.

    Args:
        start_age (int): The starting age for the audience filter.
        end_age (int | str): The ending age for the audience filter, or "+" for no limit.
        gender (str): The gender filter; "P" includes all genders.
        zips (str): A list of ZIP codes to filter the audience by location.

    Returns:
        list: A list of dictionaries, each representing a filter with `attribute`, `operand`, and `value` keys.

    """
    try:
        tot_filters = [
            {"attribute": "start_age", "operand": "=", "value": start_age},
            {"attribute": "zip", "operand": "in", "value": zips},
        ]

        if end_age != "+":
            tot_filters.append({"attribute": "end_age", "operand": "=", "value": end_age})

        if gender != "P":
            tot_filters.append(
                {"attribute": "sex", "operand": "=", "value": gender},
            )

    except TypeError:
        logging.exception("Type error encountered.")
    except Exception:
        logging.exception("An unexpected error occurred.")
    else:
        return tot_filters


def get_est_request(dataset: dict, filter_set: dict, day_parts: dict, metrics: list) -> dict:
    """Construct a ZIP-specific estimate request for audience data.

    This function builds a dictionary to be used as a request payload for retrieving audience estimates
    based on specific demographic, geographic, and time filters. The request includes parameters for
    filtering by dataset ID, demographic filters, and specific day-part ranges. The structure is designed
    to obtain both cumulative (cume) and average quarter-hour (aqh) estimates for a target subset of ZIP codes.

    Args:
        dataset (dict): Dictionary containing dataset metadata, such as the dataset ID.
        filter_set (dict): Dictionary of filter criteria (e.g., demographics) for the request.
        day_parts (dict): Dictionary specifying start and end times for day parts.
        metrics (list): List of data points for the report.

    Returns:
        dict: A dictionary formatted for use as a request payload to retrieve audience estimates,

    """
    try:
        return {
            "dayparts": day_parts,
            "reportingDataSetIds": dataset["id"],
            "estimateTypeCodes": metrics,
            "filterSet": filter_set,
            "maxQhThreshold": "?",
            "prefExclEvalType": 0,
            "qhThreshold": "?",
            "qhThresholdEvalType": "?",
            "returnQuarterHoursInDaypart": "True",
            "returnStationIdOnly": "False",
            "includeAnalysisTotal": "True",
            "trend": "?",
        }

    except KeyError:
        logging.exception("Key error encountered.")
    except TypeError:
        logging.exception("Type error encountered.")


def get_all_but_zip_estimates(zip_est_request: dict) -> dict:
    """Retrieve and format audience estimates for a subset of ZIP codes.

    This function sends a request to the client API to retrieve audience estimates for a specified
    set of parameters focused on all ZIP codes except a specific one (as indicated by `zip_est_request`).
    The estimates are organized by station ID and include values for different types of estimates
    (e.g., cumulative and average quarter-hour estimates) as specified by the API response.

    Args:
        zip_est_request (dict): A dictionary containing the request parameters for retrieving
                                audience estimates, including filters for demographics and ZIP codes.

    Returns:
        dict: A nested dictionary where each station ID maps to its estimate types and values.

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        return {
            market_estimates["stationId"]: {
                te["estimateTypeCode"]: te["value"]
                for te in market_estimates["estimates"]
            } for market_estimates in service.getEstimate(zip_est_request)["stationEstimates"]}

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def get_day_parts(start_qh: str, end_qh: str) -> dict:
    """Generate a dictionary defining day part ranges for audience estimates.

    This function creates a dictionary representing a standard day part range, with the
    start and end days fixed from Monday to Sunday. It also includes start and end quarter-hour
    times, allowing for flexible day part intervals based on the provided quarter-hour parameters.

    Args:
        start_qh (str): The start quarter-hour in 'HHMM' format.
        end_qh (str): The end quarter-hour in 'HHMM' format.

    Returns:
        dict: A dictionary with keys for start and end days of the week, the specified start and end quarter-hour times.

    """
    try:
        return {
            "startDayOfWeek": "MON",
            "endDayOfWeek": "SUN",
            "startQuarterHourOfDay": start_qh,
            "endQuarterHourOfDay": end_qh,
        }

    except KeyError:
        logging.exception("Key error encountered.")
    except TypeError:
        logging.exception("Type error encountered.")


def get_audience_metrics(tot_market_estimates: dict, all_but_zip_estimates: dict, stat_info: dict, demos: dict,
                         metrics: list) -> dict:
    """Calculate and compile final audience metrics and station demographic information.

    This function generates a dictionary containing audience estimates, demographic details,
    and station information for a specified station. The function calculates adjusted `cume`
    and `aqh` metrics by subtracting ZIP-excluded estimates from total market estimates to
    isolate ZIP-specific data. It also adds metadata like station name, demographic range,
    market identifiers, and other relevant attributes.

    Args:
        tot_market_estimates (dict): A dictionary with total market audience estimates by station.
        all_but_zip_estimates (dict): Audience estimates excluding ZIP-specific data.
        stat_info (dict): Contains station metadata.
        demos (dict): A dictionary of demographic information.
        metrics (list): List of data points for the report.

    Returns:
        dict: A dictionary with adjusted audience metrics.

    """
    try:
        stat = demos["stat"]
        if args.rpt_period == "HOLIDAY":
            survey_date = f"{args.rpt_year}-12-21"
        elif args.rpt_period in ["SPRING", "SUMMER", "FALL", "WINTER"]:
            survey_date = f"{args.rpt_year}-{diary_dict[args.rpt_period]['month_num']}-01"
        else:
            survey_date = f"{args.rpt_year}-{args.rpt_period}-01"

        return_metrics = {'zip_code': demos['zip'], 'station': stat, 'name': stat_info[stat]['name'],
                          'callLetters': stat_info[stat]['callLetters'],
                          'band': stat_info[stat]['band'], 'gender': demos['sex'], 'start_age': demos['start_age'],
                          'end_age': 100 if demos['end_age'] == '+' else demos['end_age'], 'survey_date': survey_date,
                          'reportPeriodName': demos['report_period_name'],
                          'age_range': str(demos['start_age']) + '-' + str(demos['end_age']),
                          'marketName': demos['market_name'], 'marketCode': demos['market_code'],
                          'start_qh_str': str(demos['start_qh']), 'end_qh_str': str(demos['end_qh']),
                          'survey_name': demos['survey_name'], 'day_part': 'Mon-Sun'}

        for metric in metrics:
            # If metric not in the station data, add dummy value.
            if metric not in tot_market_estimates[stat]:
                return_metrics[metric] = "-"
            else:
                # Extract values and handle NaNs for readability
                stat_metric_total = tot_market_estimates[stat][metric]
                stat_metric_all_but_zip = all_but_zip_estimates[stat][metric]

                # Convert to 0 if NaN
                total_value = 0 if math.isnan(float(stat_metric_total)) else stat_metric_total
                all_but_zip_value = 0 if math.isnan(float(stat_metric_all_but_zip)) else stat_metric_all_but_zip

                # Calculate and store the result
                return_metrics[metric] = total_value - all_but_zip_value

    except KeyError:
        logging.exception("Key error occurred.")
    except TypeError:
        logging.exception("Type error occurred.")
    except ValueError:
        logging.exception("Value error occurred.")
    except AttributeError:
        logging.exception("Attribute error occurred.")
    else:
        return return_metrics


def get_end_results(data_tup: tuple, demos: dict, metrics: list) -> list:
    """Process zip code and station demographics to generate final estimates for market data.

    This function iterates through provided zip tuples, retrieving market estimates for specified
    demographic data. It constructs filtering criteria, retrieves estimates for "all but" selected
    zips, and formats results for each station's demographic. It updates each result with holiday
    information if applicable, and returns a list of processed demographic data.

    Args:
        data_tup (tuple): A tuple containing:
            - zip_tups (list): List of tuples with zip code and zip-specific filters.
            - dataset (dict): Dataset information for reporting.
            - tot_market_estimates (dict): Market estimates for each station.
            - stat_info (dict): Station information, such as name and call letters.
        demos (dict): Demographic details including age range, gender, metrics and survey information.
        metrics (list): List of data points for the report.

    Returns:
        list: A list of dictionaries containing final market estimate data, processed for each zip and station.

    """
    try:
        end_results = []
        zip_tups, dataset, tot_market_estimates, stat_info = data_tup
        day_parts = get_day_parts(demos['start_qh'], demos['end_qh'])

        for z, all_but in zip_tups:

            # Define filters with specific zip codes, start and end ages
            zip_filters = get_filters(demos['start_age'], demos['end_age'], demos['sex'], all_but)
            filter_set = {"joinType": "AND", "requestFilters": zip_filters}

            # Build zip-specific estimate request
            zip_est_request = get_est_request(dataset, filter_set, day_parts, metrics)

            # Retrieve estimates for "all but" selected zips
            all_but_zip_estimates = get_all_but_zip_estimates(zip_est_request)

            # Process and store final data for each station
            for stat in tot_market_estimates:
                # Update the demographics dict with station and zip code.
                demographics = {**demos, 'zip': z, 'stat': stat}

                # Build zip_final dict for station demographic
                zip_final = get_audience_metrics(tot_market_estimates, all_but_zip_estimates, stat_info, demographics,
                                                 metrics)

                # Append the zip_final dict to the end_results list
                end_results.append(zip_final)

    except KeyError:
        logging.exception("Key error occurred.")
    except TypeError:
        logging.exception("Type error occurred.")
    except ValueError:
        logging.exception("Value error occurred.")
    except AttributeError:
        logging.exception("Attribute error occurred.")
    else:
        return end_results


def get_report_file_path(market_code: str, market_name: str) -> str:
    """Generate a standardized file path for a report.

    The file name is constructed by concatenating the report type, year, period, market code,
    and a sanitized version of the market name. Spaces are replaced with hyphens, and commas
    and periods are removed. The file extension is determined by the specified report format.

    Args:
        market_code (str): The market code to include in the file name.
        market_name (str): The market name to include in the file name.

    Returns:
        str: The full file path for the generated report file.

    """
    file_format = file_formats[args.rpt_format]["ext"]
    file_name = f"full-week_{args.rpt_year}-{args.rpt_period.lower()}_{market_code}_{market_name.lower()}"
    file_name = f'{file_name.replace(" ", "-").translate(str.maketrans("", "", ",."))}.{file_format}'
    return f"{DATA_DIR}/{file_name}"


def get_demographics(demo_params: tuple, ds: dict) -> dict:
    """Construct a dictionary of demographic information based on the provided parameters.

    Args:
        demo_params (tuple): A tuple containing the demographic parameters:
            - gender (str): The gender of the demographic group.
            - start_age (int): The starting age for the demographic group.
            - end_age (int or str): The ending age for the demographic group, or "+" if no upper limit.
            - start_qh (int): The start quarter-hour time slot.
            - end_qh (int): The end quarter-hour time slot.
        ds (dict): A dictionary containing market-specific information with the following keys:
            - marketName (str): The name of the market.
            - surveyName (str): The name of the survey.
            - marketCode (str): The code identifying the market.
            - reportPeriodName (str): The name of the report period.

    Returns:
        dict: A dictionary with the constructed demographic information.

    """
    gender, start_age, end_age, start_qh, end_qh = demo_params
    return {
        "sex": gender,
        "start_age": start_age,
        "end_age": end_age,
        "market_name": ds["marketName"],
        "survey_name": ds["surveyName"],
        "start_qh": start_qh,
        "end_qh": end_qh,
        "market_code": ds["marketCode"],
        "report_period_name": ds["reportPeriodName"],
    }


def get_filter_set(start_age: int, end_age: int | str, gender: str, all_zips: list) -> dict:
    """Create a filter set for demographic queries based on age, gender, and location.

    Args:
        start_age (int): The starting age for the demographic filter.
        end_age (int | str): The ending age for the demographic filter, which can be an integer
                             or a string (e.g., "+" for open-ended age ranges).
        gender (str): The gender for the demographic filter.
        all_zips (list): A list of zip codes to include in the filter.

    Returns:
        dict: A dictionary containing the filter configuration with "joinType" set to "AND"
              and "requestFilters" generated from the input parameters.

    """
    return {
        "joinType": "AND",
        "requestFilters": get_filters(start_age, end_age, gender, ",".join(all_zips)),
    }


def get_final_results(completed_tasks: Queue, ds: list) -> str:
    """Process a completed queue and a list of datasets to generate a tabulation of results.

    This function checks each dataset's market code against a queue containing market codes
    of successfully completed tasks. It returns a string that tabulates the success or failure
    of processing each dataset based on whether its market code is present in the completed queue.

    Args:
        completed_tasks (Queue): A Queue that holds market codes of datasets that have been successfully processed.
        ds (list): A list of dictionaries, where each dictionary represents a dataset.

    Returns:
        str: A tabulated string of market names and codes, along with their respective success or failure status.

    """
    completed_successfully = []
    tabulation_str = f"\n\nReporting Date: {datetime.now(tz=tz.tzlocal()).strftime('%Y.%m.%d')}\n"
    while not completed_tasks.empty():
        completed_successfully.append(completed_tasks.get())
    for data in ds:
        outcome = 'successful' if data['marketCode'] in completed_successfully else 'failed'
        tabulation_str += f"{data['marketName']}_{data['marketCode']}\t{outcome}\n"

    return tabulation_str


def get_zipcode_tuples(dataset: dict) -> list:
    """Generate tuples of zip codes and their corresponding 'all but' zip codes for the specified dataset.

    Args:
        dataset (dict): A dictionary containing information about the dataset for which zip codes are requested.

    Returns:
        A list of tuples, each containing a zip code and a comma-separated string of all other zip codes.

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        # Fetch all zip codes from the client using the dataset
        all_zips = [g["zipString"] for g in service.getGeography(dataset)["geographies"]]

        # Remove duplicate entries
        all_zips = list(set(all_zips))

        # Create tuples of each zip code and the remaining zip codes
        return [(z, ",".join([x for x in all_zips if x != z])) for z in all_zips]

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except exceptions.ValidationError:
        logging.exception("Validation error.")
    except exceptions.TransportError:
        logging.exception("Transport error.")
    except exceptions.XMLSyntaxError:
        logging.exception("XML parsing error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except TypeError:
        logging.exception("Type error.")
    except ValueError:
        logging.exception("Value error.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def set_dataset(dataset: dict) -> None:
    """Load a specified dataset into the client's environment.

    Args:
        dataset (dict): The dataset to be loaded.

    Return:
        None

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        service.loadDataSet(dataset)

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except exceptions.ValidationError:
        logging.exception("Validation error.")
    except exceptions.TransportError:
        logging.exception("Transport error.")
    except exceptions.XMLSyntaxError:
        logging.exception("XML parsing error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except TypeError:
        logging.exception("Type error.")
    except ValueError:
        logging.exception("Value error.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def process_demographic(stat_info: dict, demographics: dict, end_results_params: tuple) -> list:
    """Execute a loop to process market data and demographics, generating a list of results.

    This function deconstructs the `end_results_params` tuple into individual elements, retrieves
    total market estimates, and condenses the data into a single tuple for further processing.
    It then calls `get_end_results` to calculate and return processed results.

    Args:
        stat_info (dict): Station information data.
        demographics (dict): Demographic data to be used in processing.
        end_results_params (tuple): A tuple containing:
            - zip_tups: Tuple of zip codes to be processed.
            - dataset: The dataset containing market information.
            - filter_set: Filters applied to the dataset.
            - day_parts: Time segments for querying.
            - metrics: Metrics used in market estimation.

    Returns:
        list: The processed end results from `get_end_results` function.

    """
    # Deconstruct 'end_results_params' for processing.
    zip_tups, dataset, filter_set, day_parts, metrics = end_results_params

    # Fetch total market estimates
    tot_market_estimates = get_tot_market_estimates(dataset, filter_set, day_parts, metrics)

    # Condense the data for transfer to processing.
    data_tup = (zip_tups, dataset, tot_market_estimates, stat_info)

    # Process station single demographic
    return get_end_results(data_tup, demographics, metrics)


def run_queries(ds: dict, dc: dict, zip_tups: list, completed_tasks: Queue) -> None:  # noqa: C901, PLR0912
    """Execute a series of queries to retrieve demographic estimates based on specified filters.

    This function processes various combinations of demographic and geographic parameters to
    calculate audience estimates for specific radio stations. It organizes results and writes
    them to a CSV file. The processing steps include:

    1. Extracting market and station information.
    2. Defining day part ranges based on quarter-hour intervals.
    3. Looping through demographic combinations (gender, age ranges) and ZIP codes.
    4. Constructing and sending requests to retrieve total market estimates and estimates
       for specific ZIP codes.
    5. Calculating differences between total estimates and ZIP-specific estimates.
    6. Formatting the results and storing them for later output.

    Args:
        ds (dict): A dictionary containing metadata about the dataset, including market and survey information.
        dc (dict): A dictionary containing demographic parameters for processing.
        zip_tups (list): A list of ZIP code tuples to process for estimates.
        completed_tasks (Queue): Where to store completed report ids.

    Returns:
        None

    """
    try:
        # Construct file path.
        file_path = get_report_file_path(ds["marketCode"], ds["marketName"])

        # Flag for writing headers within the csv file.
        first_write = True

        # Retrieve station information
        stat_info = get_station_info(ds)

        # Loop over start and end quarter-hour time slots to define day part ranges
        for start_qh, end_qh in product(dc['full_week_parts'], repeat=2):
            # Only run if end date is greater than start date.
            if end_qh >= start_qh:

                # Define day part ranges
                day_parts = get_day_parts(start_qh, end_qh)

                for gender, start_age, end_age in product(dc['gender_list'], dc['start_ages_list'],
                                                          dc['end_ages_list_plus']):

                    # Process only valid age combinations.
                    if end_age != "+" and end_age >= start_age:

                        # Construct demographic dictionary.
                        demo_params = (gender, start_age, end_age, start_qh, end_qh)
                        demographics = get_demographics(demo_params, ds)

                        # Create filter set for the demographic.
                        filter_set = get_filter_set(start_age, end_age, gender, list(zip_tups[0]))

                        # Package parameters and retrieve result
                        end_results_params = (zip_tups, ds, filter_set, day_parts, dc['metrics'])
                        end_results = process_demographic(stat_info, demographics, end_results_params)

                        # Write results and update first_write flag
                        write_to_file(ds, end_results, dc['metrics'], first_write=first_write)
                        first_write = False

        # Send the final report file to S3
        send_to_s3(file_path)

    except KeyError:
        logging.exception("Key error occurred.")
    except ValueError:
        logging.exception("Value error occurred.")
    except TypeError:
        logging.exception("Type error occurred.")
    except IndexError:
        logging.exception("IndexError error occurred.")
    except FileNotFoundError:
        logging.exception("FileNotFoundError error occurred.")
    except PermissionError:
        logging.exception("PermissionError Error occurred.")
    else:
        # Log that the current dataset completed successfully.
        # Basic operations like put() and get() are already thread-safe.
        completed_tasks.put(ds["marketCode"])
    finally:
        # Delete the local report file.
        if file_path and Path(file_path).exists():
            Path(file_path).unlink()


def unload_all() -> None:
    """Unload all datasets currently loaded in the specified client.

    This function retrieves the list of loaded reporting datasets from the client and iterates
    through each dataset, calling the `unload_dataset` function to unload them. It ensures that
    no loaded datasets remain in the client's memory.

    Returns:
        None

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        if service.getLoadedDataSets() is not None:
            for data in service.getLoadedDataSets()["reportingDataSets"]:
                unload_dataset(data)

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def unload_dataset(dataset: dict) -> None:
    """Unload a specified dataset from the client.

    This function calls the client's `unloadDataSet` method to remove the given dataset from
    the client's memory. This is typically used to free up resources when a dataset is no
    longer needed for processing.

    Args:
        dataset(dict): A dataset identifier that specifies which dataset to unload.

    Returns:
        None

    """
    try:
        # Retrieve instance of Tally client.
        tally_client = TallyClient(is_local=IS_LOCAL, tally_server=TALLY_SERVER)
        service = tally_client.get_service()

        service.unloadDataSet(dataset)

    except exceptions.Fault:
        logging.exception("SOAP fault error.")
    except requests.exceptions.ConnectionError:
        logging.exception("Failed to connect to the server.")
    except requests.exceptions.Timeout:
        logging.exception("Request timed out.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def validate_env() -> None:
    """Validate the presence of required environment variables and raises an error if any are missing.

    This function checks that the necessary environment variables are set.
    If any of these variables are not defined, a `MissingEnvironmentVariableError` is raised,
    indicating which variable is missing.

    Returns:
        None

    """
    required_env_vars = {
        "DATA_DIR": DATA_DIR,
        "MAX_THREADS": MAX_THREADS,
        "IS_LOCAL": IS_LOCAL,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "BUCKET_NAME": BUCKET_NAME,
        "S3_PREFIX": S3_PREFIX,
        "TALLY_SERVER": TALLY_SERVER,
    }

    # Check each environment variable and raise an error if missing
    for var_name, var_value in required_env_vars.items():
        if not var_value:
            raise MissingEnvironmentVariableError(var_name)


def write_to_file(ds: dict, end_results: list, metrics: list, first_write: bool) -> None:
    """Write demographic data to a CSV file based on the provided dataset and results.

    This function prepares a file name based on market information and writes a collection of
    demographic results to a CSV file. If this is the first time writing to the file, it also
    includes a header row. The function handles the potential for file write errors and prints
    any encountered exceptions.

    Args:
        ds (dict): A dictionary containing metadata about the dataset, including market
                        name, market code, and report period name.
        end_results (list): A list of dictionaries containing the demographic data to be written
                            to the CSV file. Each dictionary should contain the specified field
                            names for proper formatting.
        metrics (list): List of report specific datapoints.
        first_write (bool, optional): A flag indicating whether this is the first write operation
                                       to the file. Defaults to False.

    Returns:
        None

    """
    try:
        # Prepare file name
        market_name = ds["marketName"]
        market_code = ds["marketCode"]

        # Construct file name.
        file_path = get_report_file_path(market_code, market_name)

        # Column headers for the report csv/tsv
        col_names = ["station", "callLetters", "band", "name", "zip_code", "marketName", "marketCode",
                     "reportPeriodName", "gender", "start_age", "end_age", "age_range", "start_qh_str",
                     "end_qh_str", "survey_name", "day_part", "survey_date"]

        # Open the specified file in append mode
        with Path(file_path).open("a") as report_file:

            # Initialize a CSV writer with specified column headers
            writer = csv.DictWriter(
                report_file,
                fieldnames=[*col_names, *metrics],
                delimiter=file_formats[args.rpt_format]['delim'],
            )

            # Write header row to file if this is the first write operation
            if first_write:
                writer.writeheader()

            # Write all rows in `end_results` to the CSV file
            writer.writerows(end_results)

    except KeyError:
        logging.exception("Key error occurred.")
    except ValueError:
        logging.exception("Value error occurred.")
    except TypeError:
        logging.exception("Type error occurred.")
    except OSError:
        logging.exception("OS error occurred.")
    except AttributeError:
        logging.exception("Attribute error occurred.")
    except csv.Error:
        logging.exception("A csv.Error occurred.")


def process_task(ds: dict, dc: dict, completed_tasks: Queue) -> None:
    """Execute the full data processing workflow for a given dataset.

    This function performs the following steps:
    1. Loads the dataset using the provided client.
    2. Retrieves and processes zipcode tuples.
    3. Executes queries based on demographic parameters and zipcode data.
    5. Unloads the dataset from the client.
    6. Logs the total processing time in a timing log file.

    Args:
        ds (dict): A dictionary containing metadata about the dataset.
        dc (dict): A dictionary containing demographic parameters.
        completed_tasks (Queue): Where to store completed report ids

    Returns:
        None

    """
    try:
        # Remember process start time.
        start_time = p_counter()

        # Load the dataset for this thread.
        set_dataset(ds)

        # Get the zip results for this thread.
        zip_tups = get_zipcode_tuples(ds)

        # Send to processing.
        # Log report starting.
        logging.info("Starting Report for %s.", ds['id'])
        run_queries(ds, dc, zip_tups, completed_tasks)

        # Remove the loaded data sets.
        unload_dataset(ds)

        # Calculate elapsed time for the process.
        full_time = int(p_counter() - start_time)

        # Log times to the log file.
        with Path("process_logs.txt").open("a") as report_file:
            report_file.write(f"{ds['id']} took {full_time} seconds\n")

    except KeyError:
        logging.exception("Key error occurred.")
    except TypeError:
        logging.exception("Type error occurred.")
    except OSError:
        logging.exception("OS error occurred when accessing the file.")
    except Exception:  # Catch-all for any other exceptions
        logging.exception("An unexpected error occurred.")


def get_parsed_arguments(report_types: list, report_periods: list, report_format: list) -> argparse.Namespace:
    """Parse command-line arguments for the report generation script.

    - `-f` / `--file-format`: Optional. File output format: csv|tsv
    - `-t` / `--type`: Required. Type of report to process. (ppm, diary)
    - `-p` / `--period`: Optional. Numeric value of the month (default: current month) or one of the four seasons.
    - `-y` / `--year`: Optional. Year of the report as a four-digit integer (default: current year).
    - `-m` / `--market`: Optional. Market ID for specific reporting, as a string (default: all markets).
    - `--skip-s3`: Skip sending files to S3 bucket.

    Args:
        report_types (list): List of allowable report types.
        report_periods (list): List of allowed time periods.
        report_format (list): List of allowed file formats.

    Returns:
        argparse.Namespace

    """
    try:
        # Parse command-line arguments.
        today = datetime.now(tz=tz.tzlocal())
        parser = argparse.ArgumentParser()

        parser.add_argument('-f', '--file-format', required=False, dest='rpt_format', default='csv',
                            help='File output format: csv|tsv', type=str.lower, choices=report_format)
        parser.add_argument('-t', '--type', choices=report_types, dest='rpt_type', required=True, type=str.lower,
                            help='The type of report to run. (ppm, diary)')
        parser.add_argument('-p', '--period', choices=report_periods, dest='rpt_period', required=False, type=str.upper,
                            help="""Numeric value for the month of the report. (1, 2...) or
                            for Diary reports a season. (spring, summer, fall, winter)""")
        parser.add_argument('-y', '--year', required=False, dest='rpt_year', type=int, default=today.strftime('%Y'),
                            help='The year of the report. (2023, 2024...)')
        parser.add_argument('-m', '--market', dest='market', required=False, type=str, default=None,
                            help='A single market ID for reporting.')
        parser.add_argument('--skip-s3', required=False, dest='skip_s3', action='store_true',
                            help='Skip sending files to S3 bucket')

        # Log the loaded command line arguments.
        logging.info("Parsed Arguments: %s", parser.parse_args())

    except SystemExit:
        logging.exception("Argument parsing failed with error.")
    except Exception:  # General exception to catch any other issues
        logging.exception("An unexpected error occurred while parsing arguments.")
    else:
        return parser.parse_args()


def validate_cli_args() -> None:
    """Validate command-line arguments based on the report type specified.

    For the "diary" report type, ensures the `month` argument matches one of the allowed
    period values ('SPRING', 'SUMMER', 'FALL', 'WINTER', 'HOLIDAY'). For the "ppm" report type,
    ensures the `month` argument is a numeric value within the range 1 to 12.

    Logs an informational message and exits the program with a status of 0 if validation fails.
    """
    if args.rpt_type.lower() == "diary":
        allowed_periods = ['SPRING', 'SUMMER', 'FALL', 'WINTER']
        if args.rpt_period.upper() not in allowed_periods:
            logging.warning("Report type Diary requires a period value of one of the following: %s",
                            ','.join(allowed_periods))
            sys.exit(0)

    if args.rpt_type.lower() == "ppm":
        allowed_periods = ["HOLIDAY"] + [str(i) for i in range(1, 13)]
        if args.rpt_period not in allowed_periods:
            logging.warning("Report type PPM requires a period value between 1 and 12 or HOLIDAY")
            sys.exit(0)


def worker(task_queue: Queue) -> None:
    """Continuously retrieves and processes tasks from the task_queue until it is empty.

    For each task, retrieves parameters, executes 'process_task' with those parameters.
    """
    while not task_queue.empty():
        params = task_queue.get()
        logging.info("Tasks Remaining in the Queue: %s", task_queue.qsize())
        try:
            process_task(*params)
        except Exception:  # Catch any exception that occurs during processing
            logging.exception("An error occurred while processing task with parameters.")
        finally:
            task_queue.task_done()


def send_to_s3(filename: str) -> None:
    """Send files to S3 bucket.

    Upload completed file report to S3.

    Args:
        filename (str): Local filename to be sent to S3.

    Returns:
        None

    """
    try:
        if not args.skip_s3:
            logging.info('Starting S3 copy of: %s', str(filename).split('/')[-1])
        else:
            logging.info("Skipping upload to S3 of: %s.", str(filename).split('/')[-1])
            return

        # Create an S3 client.
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # Create the respective URLs.
        s3_file_name = str(filename).split('/')[-1]
        archive_url = f"{S3_PREFIX}/archive/{args.rpt_year}/{args.rpt_period.lower()}/zipcodes/{s3_file_name}"
        snowflake_url = f"{S3_PREFIX}/to_snowflake/zipcodes/{s3_file_name}"

        # Send the local file to S3.
        s3_client.upload_file(filename, BUCKET_NAME, archive_url)
        s3_client.upload_file(filename, BUCKET_NAME, snowflake_url)

        logging.info('S3 copy completed: %s', str(filename).split('/')[-1])

    except NoCredentialsError:
        logging.exception("AWS credentials not found. Please check your AWS configuration.")
    except PartialCredentialsError:
        logging.exception("Incomplete AWS credentials. Both access key and secret key are required.")
    except EndpointConnectionError:
        logging.exception("Could not connect to S3 endpoint.")
    except ClientError:
        logging.exception("S3 ClientError.")
    except FileNotFoundError:
        logging.exception("The specified file was not found: %s", filename)
    except Exception:
        logging.exception("An unexpected error occurred.")


def main() -> None:
    """Configure and process demographic datasets for radio audience metrics.

    This function organizes and prepares demographic configurations and market data,
    loads datasets, and initiates parallel processing of audience metrics by creating
    worker threads. Configurations for demographics, genders, metrics, and reporting periods
    are based on the report type and user input arguments. Results are logged and recorded.

    Steps:
        - Set demographic configurations, including age, gender, and reporting periods.
        - Filter datasets based on report type and market, and sort by market size.
        - Load datasets into a queue, starting with larger markets.
        - Use ThreadPoolExecutor to initiate parallel processing by worker threads.
        - Monitor queue processing and calculate total execution time.
        - Record and log results, handling any missing datasets.

    Outputs:
        - Logs total processing time and results to a text file.
        - Displays final processing results on the console.

    Returns:
        None

    """
    try:
        # Set report specific parameters.
        if args.rpt_type == "diary":
            # Trim youngest ages from demographic.
            start_ages_list.pop(0)
            end_ages_list.pop(0)

            # Create a file identifier string for filtering datasets.
            file_code = diary_dict[args.rpt_period.upper()]['file_code']
            identifier = re.compile(f"radio-{file_code}.*")

            # Define friendly name for the reporting period.
            period = args.rpt_period[:3]

            # Add market if applicable
            file_identifier = re.compile(f"radio-{file_code}.*-{args.market}$") if args.market else identifier

        elif args.rpt_type == "ppm":
            # Define period, if HOLIDAY or month.
            period = "HOL" if args.rpt_period == "HOLIDAY" else month_list[int(args.rpt_period)]['short']

            # Create a file identifier string for filtering datasets.
            identifier = f"PPM-{period}-{args.rpt_year}"

            # Add market if applicable
            file_identifier = f"{identifier}-{args.market}" if args.market else identifier

        else:  # Return error for unknown report type.
            logging.info("Report type %s is not a recognized report type.", args.rpt_type)
            sys.exit(0)

        # Basic demographic setup that should remain constant throughout the processing.
        demographic_config = {
            "gender_list": gender_dict.keys(),  # Gender options: Male, Female, and P for either.
            "start_ages_list": start_ages_list,  # Starting ages for demographic segments.
            "end_ages_list_plus": end_ages_list,  # Corresponding ending ages for the segments.
            "full_week_parts": ["0600", "2400"],  # Hard coded full_week report.
            "metrics": metric_list,  # Report metrics list.
            "period": period,  # Friendly name for the reporting period.
        }

        # Retrieve the list of loadable datasets based on the specified month and year.
        loadable_datasets = get_dataset_list(file_identifier)
        logging.info("Datasets Loaded for Processing: %s",
                     len(loadable_datasets))  # Output the available datasets for processing.

        # Exit if nothing to do.
        if len(loadable_datasets) < 1:
            logging.info("No loadable datasets.  Exiting Process.")
            sys.exit(0)

        # Create a lookup dictionary for marketCode positions
        market_order = {code: idx for idx, code in enumerate(sorted_markets)}

        # Sort 'loadable_datasets' on the index of 'marketCode' in 'market_order' with a default value for missing ids.
        loadable_datasets = sorted(loadable_datasets, key=lambda d: market_order.get(d["marketCode"], float('inf')))

        # Unload any previously loaded datasets from the Tally Engine to start fresh.
        unload_all()

        # Instantiate the task queue
        task_queue = Queue()

        # Instantiate the completed queue
        completed_queue = Queue()

        # Populate the task_queue
        for d_set in loadable_datasets:
            task_queue.put((
                d_set,  # Current dataset being processed
                demographic_config,  # Basic demographic setup
                completed_queue,  # Queue to record successful completions.
            ))

        # Record the start time for the processing operation.
        process_start_time = p_counter()

        # Start the ThreadPoolExecutor with MAX_THREADS number of workers.
        with ThreadPoolExecutor(max_workers=int(MAX_THREADS)) as executor:
            for _ in range(int(MAX_THREADS)):
                executor.submit(worker, task_queue)

        # Wait until all tasks in the queue are processed
        task_queue.join()

        # Calculate the total processing time and log it to a CSV file for future reference.
        total_process_time = int(p_counter() - process_start_time)  # Time taken for the full process

        # Process the results for any missing datasets.
        results = get_final_results(completed_queue, loadable_datasets)

        # Open the process_logs file in append mode
        with Path("process_logs.txt").open("a") as out_file:

            # Write the time taken
            out_file.write(f"Full process took {total_process_time} seconds\n")

            # Write process results
            out_file.write(f"\n{args}")
            out_file.write(f"{results}\n")

        # Print final results to the console.
        print(results)

    except (FileNotFoundError, PermissionError):
        logging.exception("File error.")
    except (Empty, Full):
        logging.exception("Queue error.")
    except Exception:
        logging.exception("An unexpected error occurred.")


if __name__ == "__main__":
    try:
        # Validate needed ENVARS are present in the env.
        validate_env()

        # Convert IS_LOCAL to a boolean value.
        IS_LOCAL = IS_LOCAL == "True"

        # Markets list sorted by size of market.  Used to start the larger markets first in the threading.
        sorted_markets = ['001', '005', '003', '024', '033', '015', '013', '009', '039', '429', '027', '011',
                          '017', '023', '035', '041', '057', '051', '007', '021', '043', '019', '135', '065',
                          '063', '257', '077', '061', '131', '109', '379', '166', '075', '047']

        # Configure age metrics
        start_ages_list = [6, 12, 18, 25, 35, 45, 50, 55, 65]
        end_ages_list = [11, 17, 24, 34, 44, 49, 54, 64, '+']

        # Configure the gender dictionary.
        gender_dict = {'M': 'Male', 'F': 'Female', 'P': 'Persons'}

        # Configure months dictionary.
        month_list = [
            {'short': 'DMB'}, {'short': 'JAN'}, {'short': 'FEB'}, {'short': 'MAR'}, {'short': 'APR'}, {'short': 'MAY'},
            {'short': 'JUN'}, {'short': 'JUL'}, {'short': 'AUG'}, {'short': 'SEP'}, {'short': 'OCT'}, {'short': 'NOV'},
            {'short': 'DEC'}]

        # Doing it like this as future proofing for addition metrics.  Be aware that some common math is done on all
        # properties within this list in the 'get_audience_metrics' function.
        metric_list = ['aqh', 'cume']

        # Configure allowed  file formats.
        file_formats = {
            'csv': {'delim': ',', 'ext': 'csv'},
            'tsv': {'delim': '\t', 'ext': 'tsv'},
        }

        # Configure Seasons dictionary
        diary_dict = {
            'WINTER': {'file_code': 'W', 'month_num': '03'},
            'SPRING': {'file_code': 'S', 'month_num': '06'},
            'SUMMER': {'file_code': 'U', 'month_num': '09'},
            'FALL': {'file_code': 'F', 'month_num': '12'},
        }

        # Configure allowed report types.
        rpt_types = ["ppm", "diary"]

        # Configure allowed Report formats
        rpt_format = ['csv', 'tsv']

        # Configure allowed report periods
        rpt_periods = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'SPRING', 'SUMMER', 'FALL',
                       'WINTER', 'HOLIDAY']

        # Process command line arguments.
        args = get_parsed_arguments(rpt_types, rpt_periods, rpt_format)

        # Validate cli arguments and their combinations.
        validate_cli_args()

        # Run 'main' process
        main()

    except KeyError:
        logging.exception("Environment variable missing or invalid configuration in dictionaries.")
    except AttributeError:
        logging.exception("Attribute error: Possibly due to a missing or invalid IS_LOCAL value.")
    except (TypeError, ValueError):
        logging.exception("Argument type or value error.")
    except Exception:
        logging.exception("An unexpected error occurred.")
