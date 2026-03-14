import shlex
import re
import gzip
import shutil
import requests
import subprocess
from datetime import datetime as dt
from functools import wraps
from pathlib import Path
from tqdm import tqdm


DATA_DIR = Path("../data")
FILE_NAME = Path("NASA_access_log_Jul95")
LOG_PATTERN = re.compile(r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\d+|-)')
DOWNLOAD_URL = "https://raw.githubusercontent.com/scalable-infrastructure/exercise-2026/main/data/nasa/NASA_access_log_Jul95.gz"


def record_time(func):
    @wraps(func)
    def timer(*args, **kwargs):
        time = dt.now()
        result = func(*args, **kwargs)
        time = dt.now() - time
        return result, time
    
    return timer


def run_cmd(func, shell=True, text=True, capture_output=True, check=True):
    @wraps(func)
    def runner(*args, **kwargs) -> str:
        cmd = func(*args, **kwargs)
        result = subprocess.run(
            cmd,
            shell=shell,
            text=text,
            capture_output=capture_output,
            check=check
        )
        return result.stdout.strip()

    return runner


def quote_path(path: Path | str) -> str:
    return shlex.quote(str(path))


@run_cmd
def preview_log_head(log_file: Path, n: int = 5) -> str:
    file_ = quote_path(log_file)
    return f"head -n {int(n)} {file_}"


@run_cmd
def count_log_lines(log_file: Path) -> str:
    file_ = quote_path(log_file)
    return f"wc -l {file_}"


@run_cmd
def locate_log_file(data_dir: Path = DATA_DIR, file_name: Path = FILE_NAME) -> str:
    dir_ = quote_path(data_dir)
    name_ = shlex.quote(str(file_name))
    return f"find {dir_} -type f -name {name_}"


@run_cmd
def count_lines_with_find_xargs(data_dir: Path = DATA_DIR, file_name: Path = FILE_NAME) -> str:
    dir_ = quote_path(data_dir)
    name_ = shlex.quote(str(file_name))
    return f"find {dir_} -type f -name {name_} | xargs wc -l"


@run_cmd
def get_most_called_page(log_file: Path) -> str:
    file_ = quote_path(log_file)
    return (
        f"cat {file_} "
        r"""| awk '{print $7}' """
        r"""| sort """
        r"""| uniq -c """
        r"""| sort -nr """
        r"""| head -1"""
    )


@run_cmd
def get_most_frequent_return_code(log_file: Path) -> str:
    file_ = quote_path(log_file)
    return (
        f"cat {file_} "
        r"""| awk '{print $9}' """
        r"""| sort """
        r"""| uniq -c """
        r"""| sort -nr """
        r"""| head -1"""
    )


@run_cmd
def get_error_stats(log_file: Path) -> str:
    file_ = quote_path(log_file)
    return (
        f"cat {file_} "
        r"""| awk '$9 >= 400 {print $9}' """
        r"""| sort """
        r"""| uniq -c """
        r"""| awk 'BEGIN {total=0} {total += $1} END {print total}'"""
    )


@record_time
def main():
    path = DATA_DIR / FILE_NAME

    print("Log file location:")
    print(locate_log_file(DATA_DIR, FILE_NAME))
    print()

    print("Log preview:")
    print(preview_log_head(path, n=5))
    print()

    print("Line count (wc):")
    print(count_log_lines(path))
    print()

    print("Line count (find | xargs | wc):")
    print(count_lines_with_find_xargs(DATA_DIR, FILE_NAME))
    print()

    print("Most called page:")
    print(get_most_called_page(path))
    print()

    print("Most frequent return code:")
    print(get_most_frequent_return_code(path))
    print()

    print("Number of error responses (status >= 400):")
    print(get_error_stats(path))


if __name__ == "__main__":
    main()
    print("Done!")