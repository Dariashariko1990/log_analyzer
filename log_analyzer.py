import argparse
import gzip
import json
import os
import re
import shutil
import sys
from collections import namedtuple, defaultdict
import datetime as dt
import statistics
from imp import load_source
from operator import attrgetter
from string import Template
import logging
import tempfile

TEMPLATE = 'report.html'

DEFAULT_CONFIG = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports/",
    "LOG_DIR": "./log/",
}

LOG_FILENAME_PATTERN = re.compile(r'^nginx-access-ui.log-(\d{8}).(gz|log)$')
LOG_FORMAT_PATTERN = re.compile(r"^.+\[.+\] \"(.+)\" \d{3}.+ (\d+.\d+)")

LogFile = namedtuple('LogFile', ['path', 'date', 'extension'])
LogLine = namedtuple("LogLine", ["url", "time"])
LogStats = namedtuple("LogStat", [
    "url",
    "count",
    "count_perc",
    "time_sum",
    "time_perc",
    "time_avg",
    "time_max",
    "time_med",
],
                      )


parser = argparse.ArgumentParser(description="Passing config path")
parser.add_argument(
    "--config",
    dest="config_path",
    help="Path to config file. If not specified, script uses default config settings.",
)


def update_config(config_path, config):
    """Update default config dict from config file.
    :param config_path: path to config file
    :return: config dict
    """

    config_fromfile = load_source('config', config_path).config
    config = {**config, **config_fromfile}
    return config


def find_most_recent_log(directory):
    """Find the most recent nginx log in specified directory.
    :param directory: logs directory
    :return: LogFile named tuple
    """
    if not os.path.isdir(directory):
        logging.info("Invalid log directory, no such directory.")
        sys.exit(-1)

    most_recent_date = None
    most_recent_file = None
    most_recent_ext = None

    for file in os.listdir(directory):
        search = LOG_FILENAME_PATTERN.search(file)
        date, extension = search.groups()

        try:
            parsed_date = dt.datetime.strptime(date, "%Y%m%d")
        except ValueError:
            print("Invalid date format.")

        if most_recent_date is None or parsed_date > most_recent_date:
            most_recent_date = parsed_date
            most_recent_file = file
            most_recent_ext = extension

        recent_log_ntuple = LogFile(os.path.abspath(os.path.join(directory, most_recent_file)), most_recent_date,
                                    most_recent_ext)

        return recent_log_ntuple


def parse_log(log):
    """Generator that parse log file and yields named tuple LogLine.
    :param log: LogFile
    """

    open_method = gzip.open if log.extension == "gz" else open

    with open_method(log.path, "rb") as f:
        for line in f:
            line = line.decode("utf-8")
            search = LOG_FORMAT_PATTERN.search(line)
            if search is None:
                yield None
                continue
            try:
                method, url, protocol = search.group(1).split()
            except ValueError:
                print("Invalid log line format.")
            request_time = float(search.group(2))
            yield LogLine(url, request_time)


def count_url(log, error_threshold=0.2):
    """Parse specified log and calculate total time of all requests.
    :param log: LogFile
    :param error_threshold: acceptable error percentage
    :return: default dict with key: url, value: request time; float: total requests time
    :raise: ValueError if the percentage of errors exceeds error threshold
    """

    parsed_lines = 0.0
    invalid_lines = 0.0

    total_requests_time = 0.0
    requests_by_url = defaultdict(list)

    for line in parse_log(log):

        if line is None:
            invalid_lines += 1
            continue

        parsed_lines += 1
        total_requests_time += line.time
        requests_by_url[line.url].append(line.time)

    if invalid_lines / (invalid_lines + parsed_lines) > error_threshold:
        raise ValueError("Error threshold was exceed. More than 20% of log haven't been parsed correctly.")

    return requests_by_url, total_requests_time


def count_url_stats(requests_by_url, total_requests_time):
    """Count statistics for each url.
    :param requests_by_url: default dict with key: url, value: request time
    :param total_requests_time: float
    :return: list of named tuples LogStats
    """

    url_stats = []
    for url in requests_by_url:
        count = len(requests_by_url[url])
        count_perc = round((100 * count / len(requests_by_url)), 5)
        time_sum = round(sum(requests_by_url[url]), 5)
        time_perc = round((100 * time_sum / total_requests_time), 5)
        time_avg = round((time_sum / count), 5)
        time_max = sorted(requests_by_url[url])[-1]
        time_med = round((statistics.median(requests_by_url[url])), 5)

        url_stat = LogStats(
            url=url,
            count=count,
            count_perc=count_perc,
            time_sum=time_sum,
            time_perc=time_perc,
            time_avg=time_avg,
            time_max=time_max,
            time_med=time_med,
        )
        url_stats.append(url_stat)

    return sorted(url_stats, key=attrgetter("time_sum"), reverse=True)


def render_template(url_stats):
    """Render html template.
    :param url_stats: list of named tuples LogStats
    """
    with open(TEMPLATE, "rb") as f:
        template = Template(f.read().decode("utf-8"))
    rendered_template = template.safe_substitute(
        table_json=json.dumps([stat._asdict() for stat in url_stats])
    )
    return rendered_template


def write_report(rendered_template, report_path):
    """Write report to file.
    :param rendered_template: template string
    :param report_path: path for report file
    """
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'wb') as f:
        f.write(rendered_template.encode("utf-8"))
        with open(report_path, 'wb') as f:
           shutil.copyfileobj(tmp, f)

def main():
    args = parser.parse_args()
    if args.config_path:
        config = update_config(args.config_path, DEFAULT_CONFIG)

    log_file = config["LOG_FILE"] if "LOG_FILE" in config else None
    logging.basicConfig(filename=log_file, level=logging.INFO, format='[%(asctime)s] %(levelname).1s %(message)s')

    log = find_most_recent_log(config["LOG_DIR"])

    if log is None:
        logging.info("No logs to parse.")
        sys.exit(-1)

    try:
        report_filename = log.date.strftime("report-%Y.%m.%d.html")
        report_path = os.path.abspath(os.path.join(config["REPORT_DIR"], report_filename))

        if not os.path.isdir(config["REPORT_DIR"]):
            os.mkdir(os.path.abspath(config["REPORT_DIR"]))

        if os.path.exists(report_path):
            logging.info("The most recent report already exists. Script completed.")
            sys.exit(0)
        else:
            dict_url, time_all = count_url(log)
            url_stat = count_url_stats(dict_url, time_all)
            rendered_template = render_template(url_stat)
            write_report(rendered_template, report_path)
            logging.info("Report has been successfully generated and written to a file.")
            logging.info("Script completed.")
    except Exception as exc:
        logging.exception(exc)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.exception(exc)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt has been caught.")