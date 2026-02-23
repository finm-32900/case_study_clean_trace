"""Load project configurations from .env files or from the command line.

Provides easy access to paths and credentials used in the project.
Meant to be used as an imported module.

If `settings.py` is run on its own, it will create the appropriate
directories.

For information about the rationale behind decouple and this module,
see https://pypi.org/project/python-decouple/

Example
-------
Create a file called `myexample.py` with the following content:
```
from settings import config
DATA_DIR = config("DATA_DIR")

print(f"Using DATA_DIR: {DATA_DIR}")
```
and run
```
>>> python myexample.py --DATA_DIR=/path/to/data
/path/to/data
```
and compare to
```
>>> export DATA_DIR=/path/to/other
>>> python myexample.py
/path/to/other
```

"""

import sys
from pathlib import Path
from platform import system

from decouple import config as _config


def find_all_caps_cli_vars(argv=sys.argv):
    """Find all command line arguments that are all caps.

    Find all command line arguments that are all caps and defined
    with a long option, for example, --DATA_DIR or --MANUAL_DATA_DIR.
    When that option is found, the value of the option is returned.

    For example, if the command line is:
    ```
    python settings.py --DATA_DIR=/path/to/data --MANUAL_DATA_DIR=/path/to/manual_data
    ```
    Then the function will return:
    ```
    {'DATA_DIR': '/path/to/data', 'MANUAL_DATA_DIR': '/path/to/manual_data'}
    ```
    """
    result = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        # Handle --VAR=value format
        if arg.startswith("--") and "=" in arg and arg[2:].split("=")[0].isupper():
            var_name, value = arg[2:].split("=", 1)
            result[var_name] = value
        # Handle --VAR value format (where value is the next argument)
        elif arg.startswith("--") and arg[2:].isupper() and i + 1 < len(argv):
            var_name = arg[2:]
            value = argv[i + 1]
            if not value.startswith("--"):
                result[var_name] = value
                i += 1
        i += 1
    return result


cli_vars = find_all_caps_cli_vars()

########################################################
## Define defaults
########################################################
defaults = {}

# Absolute path to root directory of the project
if "BASE_DIR" in cli_vars:
    defaults["BASE_DIR"] = Path(cli_vars["BASE_DIR"])
else:
    defaults["BASE_DIR"] = Path(__file__).absolute().parent.parent


# OS type
def get_os():
    os_name = system()
    if os_name == "Windows":
        return "windows"
    elif os_name in ("Darwin", "Linux"):
        return "nix"
    else:
        return "unknown"


if "OS_TYPE" in cli_vars:
    defaults["OS_TYPE"] = cli_vars["OS_TYPE"]
else:
    defaults["OS_TYPE"] = get_os()


## File paths
def if_relative_make_abs(path):
    """If a relative path is given, make it absolute, assuming
    that it is relative to the project root directory (BASE_DIR).
    """
    path = Path(path)
    if path.is_absolute():
        abs_path = path.resolve()
    else:
        abs_path = (defaults["BASE_DIR"] / path).resolve()
    return abs_path


defaults = {
    "DATA_DIR": if_relative_make_abs(Path("_data")),
    "MANUAL_DATA_DIR": if_relative_make_abs(Path("data_manual")),
    "OUTPUT_DIR": if_relative_make_abs(Path("_output")),
    **defaults,
}


_UNSET = object()


def config(
    var_name,
    default=_UNSET,
    cast=None,
    settings_py_defaults=defaults,
    cli_vars=cli_vars,
    convert_dir_vars_to_abs_path=True,
):
    """Config defines a variable that can be used in the project. The definition of variables follows
    an order of precedence:
    1. Command line arguments
    2. Environment variables
    3. Settings.py file
    4. Defaults defined in-line in the local file
    5. Error
    """

    # 1. Command line arguments (highest priority)
    if var_name in cli_vars and cli_vars[var_name] is not None:
        value = cli_vars[var_name]
        if cast is not None:
            value = cast(value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            value = if_relative_make_abs(Path(value))
        return value

    # 2. Environment variables through decouple
    env_sentinel = object()
    env_value = _config(var_name, default=env_sentinel)
    if env_value is not env_sentinel:
        if cast is not None:
            env_value = cast(env_value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            env_value = if_relative_make_abs(Path(env_value))
        return env_value

    # 3. Settings.py defaults dictionary
    if var_name in defaults:
        default_value = defaults[var_name]
        if cast is not None:
            default_value = cast(default_value)
        return default_value

    # 4. Use the default value provided in the local file. Error if not found
    if default is not _UNSET:
        if cast is not None and default is not None:
            return cast(default)
        return default
    raise ValueError(
        f"Configuration variable '{var_name}' is not defined. "
        f"Please set it via:\n"
        f"  1. Command line: --{var_name}=value\n"
        f"  2. Environment variable: export {var_name}=value\n"
        f"  3. .env file: {var_name}=value"
    )


########################################################
## FISD universe build parameters
########################################################
FISD_PARAMS = {
    "currency_usd_only": True,
    "fixed_rate_only": True,
    "non_convertible_only": True,
    "non_asset_backed_only": True,
    "exclude_bond_types": True,
    "valid_coupon_frequency_only": True,
    "require_accrual_fields": True,
    "principal_amt_eq_1000_only": True,
    "exclude_equity_index_linked": True,
    "enforce_tenor_min": True,
    "invalid_coupon_freq": [-1, 13, 14, 15, 16],
    "excluded_bond_types": [
        "TXMU", "CCOV", "CPAS", "MBS", "FGOV", "USTC", "USBD", "USNT",
        "USSP", "USSI", "FGS", "USBL", "ABS", "O30Y", "O10Y", "O5Y",
        "O3Y", "O4W", "O13W", "O26W", "O52W", "CCUR", "ADEB", "AMTN",
        "ASPZ", "EMTN", "ADNT", "ARNT", "TPCS", "CPIK", "PS", "PSTK",
    ],
    "tenor_min_years": 1.0,
}


########################################################
## Default sample date range
########################################################
# By default the pipeline processes a small 2-month window so that
# first-time users can run end-to-end quickly without pulling 20+ years
# of TRACE data from WRDS.
#
# To process the full history, set START_DATE and END_DATE in .env:
#   START_DATE=2002-07-01
#   END_DATE=2025-12-31
#
# Can also be set via CLI or environment variables:
#   - CLI:          ipython script.py -- --START_DATE=2024-01-01 --END_DATE=2024-02-28
#   - Environment:  export START_DATE=2024-01-01
#
# Each pull script clamps START_DATE to the earliest available date
# for that dataset (e.g. 2002-07-01 for Enhanced, 2024-10-01 for Standard).
SAMPLE_START_DATE = "2024-01-01"
SAMPLE_END_DATE = "2024-02-28"


def get_start_date():
    """Return START_DATE as a datetime.date.

    Falls back to SAMPLE_START_DATE when not set in .env.
    """
    from datetime import date as _date

    val = config("START_DATE", default=None)
    if val is None or val == "":
        return _date.fromisoformat(SAMPLE_START_DATE)
    return _date.fromisoformat(str(val))


def get_end_date():
    """Return END_DATE as a datetime.date.

    Falls back to SAMPLE_END_DATE when not set in .env.
    """
    from datetime import date as _date

    val = config("END_DATE", default=None)
    if val is None or val == "":
        return _date.fromisoformat(SAMPLE_END_DATE)
    return _date.fromisoformat(str(val))


def create_directories():
    config("DATA_DIR").mkdir(parents=True, exist_ok=True)
    config("OUTPUT_DIR").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    create_directories()
