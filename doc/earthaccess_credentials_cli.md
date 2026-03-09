# seppo_earthaccess_credentials — CLI Reference

Manage NASA Earthdata S3 credentials and bearer tokens for accessing
NISAR and other Earthdata products on AWS.

---

## Usage

```
seppo_earthaccess_credentials [-h] [-s] [-u] [-D DAAC] [-C COLLECTION]
                               [-t] [-S] [-q]
```

---

## Arguments

| Argument | Description |
|----------|-------------|
| `-s`, `--set` | Fetch and print temporary AWS credentials as `export` statements. Pipe to `eval` to set them in the current shell. |
| `-u`, `--unset` | Print `unset` statements to remove AWS credential environment variables. Pipe to `eval`. |
| `-D DAAC`, `--DAAC` | DAAC short name. Use `list` to print all available DAACs. Default: `ASF`. |
| `-C COLLECTION`, `--Collection` | DAAC collection name. Default: `NISAR`. |
| `-t`, `--token` | Generate or refresh the Earthdata bearer token, cached at `~/.cache/openseppo/earthaccess_token.json`. Used by `seppo_nisar_gcov_convert` for direct HDF5 access. |
| `-S`, `--show_token` | Pretty-print the cached bearer token including expiry and validity status. |
| `-q`, `--quiet` | Quiet mode — suppresses printed credentials. Returns a dict when used from Python. |
| `-h`, `--help` | Show help and exit. |

---

## Common usage

```bash
# Set temporary S3 credentials in the current shell (ASF DAAC, NISAR collection)
eval $(seppo_earthaccess_credentials -s)

# Unset credentials
eval $(seppo_earthaccess_credentials -u)

# Generate or refresh the Earthdata bearer token
seppo_earthaccess_credentials -t

# Show cached token info (expiry, validity)
seppo_earthaccess_credentials -S

# List available DAACs
seppo_earthaccess_credentials -D list

# Set credentials for a different DAAC
eval $(seppo_earthaccess_credentials -s -D NSIDC)
```

The `set` command prints lines of the form:

```bash
export AWS_ACCESS_KEY_ID=<accessKeyId>
export AWS_SECRET_ACCESS_KEY=<secretAccessKey>
export AWS_SESSION_TOKEN=<sessionToken>
```

Wrapping the call in `eval $(...)` applies these exports to the current shell session. Credentials are temporary (typically valid for 1 hour).

---

## Full help output

```
usage: earthaccess_credentials.py [-h] [-s] [-u] [-D DAAC] [-C COLLECTION]
                                  [-t] [-S] [-q]

(Un)Setting Earthaccess AWS Environment variables

options:
  -h, --help            show this help message and exit
  -s, --set             set environment variables
  -u, --unset           unset environment variables
  -D DAAC, --DAAC DAAC  DAAC short name. Use 'list' to list available DAACs.
                        (default: ASF)
  -C COLLECTION, --Collection COLLECTION
                        DAAC Collection (default: NISAR)
  -t, --token           Generate or refresh the Earthdata bearer token
                        (cached at ~/.cache/openseppo/earthaccess_token.json)
  -S, --show_token      Pretty-print the cached Earthdata bearer token
                        including expiry and validity status.
  -q, --quiet           Quiet mode. Does not print the credentials.
```
