#!/usr/bin/env python
"""Set and unset temporary NASA Earthaccess AWS envtironment variables

(c) 2026 Earth Big Data

Usage:

# Set credentials:
eval $(seppo_earthaccess_credentials -s)

# Unset credentials
eval $(seppo_earthaccess_credentials -u)
"""

import earthaccess
import os
import argparse
import shlex
import sys

daac_s3credentials_endpoints = {
    "ASF": {
        "SENTINEL-1": "https://sentinel1.asf.alaska.edu/s3credentials",
        "NISAR": "https://nisar.asf.earthdatacloud.nasa.gov/s3credentials",
    },
}


def get_s3credentials_endpoint(args):
    if args.DAAC in daac_s3credentials_endpoints and args.Collection in daac_s3credentials_endpoints[args.DAAC]:
        return daac_s3credentials_endpoints[args.DAAC][args.Collection]
    DAACS = earthaccess.auth.DAACS
    DAACS_sn = sorted(x["short-name"] for x in DAACS)
    if args.DAAC not in DAACS_sn:
        raise ValueError(f"Invalid DAAC: {args.DAAC}")
    DAAC = [x for x in DAACS if x["short-name"] == args.DAAC][0]
    return DAAC["s3-credentials"]


def processing(args):

    if args.set:
        try:
            auth = earthaccess.login(strategy="all")
        except Exception:
            print("Cannot autthenticate with provided credentials")
            sys.exit(0)
        endpoint = get_s3credentials_endpoint(args)
        creds = auth.get_s3_credentials(endpoint=endpoint)  # AWS_ACCESS_KEY_ID      AWS_DEFAULT_REGION     AWS_SECRET_ACCESS_KEY  AWS_SESSION_TOKEN
        if not args.quiet:
            print(f"export AWS_ACCESS_KEY_ID={creds['accessKeyId']}")
            print(f"export AWS_SECRET_ACCESS_KEY={creds['secretAccessKey']}")
            print(f"export AWS_SESSION_TOKEN={creds['sessionToken']}")
        return creds

    if args.unset:
        print("unset AWS_ACCESS_KEY_ID")
        print("unset AWS_SECRET_ACCESS_KEY")
        print("unset AWS_SESSION_TOKEN")


def myargsparse(a):

    # Setup commandline parameters
    class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
        pass

    if type(a) is str:
        a = shlex.split(a)

    thisprog = os.path.basename(a[0])

    epilog = """This program prints the settings for temporary AWS credentials to access s3 resources directly when on ec2 resources in the valid AWS zone. Output is

export AWS_ACCESS_KEY_ID=<accessKeyId>
export AWS_SECRET_ACCESS_KEY=<secretAccessKey>
export AWS_SESSION_TOKEN=<sessionToken>

# Set credentials for ASF DAAC:
eval $(seppo_earthaccess_credentials -s)

# Unset credentials
eval $(seppo_earthaccess_credentials -u)
"""

    description = "(Un)Setting Earthaccess AWS Environment variables"
    p = argparse.ArgumentParser(prog=thisprog, description=description, epilog=epilog, formatter_class=CustomFormatter)
    p.add_argument("-s", "--set", required=False, help="set environment variables", action="store_true", default=False)
    p.add_argument("-u", "--unset", required=False, help="unset environment variables", action="store_true", default=False)
    p.add_argument("-D", "--DAAC", required=False, help="DAAC short name. Use 'list' to list short name and name of available DAACS", action="store", default="ASF")
    p.add_argument("-C", "--Collection", required=False, help="DAAC Collection", action="store", default="NISAR")
    p.add_argument("-q", "--quiet", required=False, help="Quiet mode. Does not print the credentials. Used when calling processing to return a dictionary with credentials to embed in other python scripts", action="store_true", default=False)

    a = p.parse_args(a[1:])

    if a.DAAC == "list":
        DAACS = sorted((x["short-name"], x["name"]) for x in earthaccess.auth.DAACS)
        for sn, n in DAACS:
            print(sn, n)
        sys.exit(0)

    if a.set and a.unset:
        p.print_usage()
        print("Choose either -s or -u")
        sys.exit(0)

    if not (a.set or a.unset):
        p.print_usage()
        print("Choose either -s or -u")

        sys.exit(0)

    return a


def _main(a):
    DEVEL = False
    if DEVEL:

        class args:
            DAAC = "ASF"
            set = True
            unset = False

    else:
        args = myargsparse(a)

    processing(args)


def main():
    _main(sys.argv)


if __name__ == "__main__":
    main()
