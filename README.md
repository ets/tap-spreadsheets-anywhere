# tap-spreadsheets-anywhere

This is a [Singer](https://singer.io) tap that reads data from spreadsheet files (CSVs, Excel, JSONs, custom-delimited) accessible from any [smart_open](https://github.com/RaRe-Technologies/smart_open) supported transport and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md). This tap is developed for compatibility with [Meltano](https://meltano.com/).

## How to use it

`tap-spreadsheets-anywhere` works together with any other [Singer Target](https://singer.io) to move data from any [smart_open](https://github.com/RaRe-Technologies/smart_open) supported transport to any target destination. [smart_open](https://github.com/RaRe-Technologies/smart_open) supports a wide range of transport options out of the box, including:

- S3
- local directories (file://) 
  - NOTE: that absolute paths look like this "file:///root/child/target" with three forward slashes
  - NOTE: on windows to point to a seperate drive letter an absolute path will not work, if you'd like to point to a folder on your `D` drive use "file://d:/data/subfolder"
- HTTP, HTTPS (read-only)
- SSH, SCP and SFTP
- WebHDFS
- GCS
- Azure Blob Storage
- IMAP (`imap://`)
  - Example path: `imap://imap.gmail.com/INBOX/*/*.csv`
- SharePoint and OneDrive (`msgd://`, `sharepoint://`, `onedrive://`)
  - Example path: `sharepoint://MySite/Documents/`

Multiple individual files with the same schema can be configured & ingested into the same "Table" for processing.

### Compression
smart_open allows reading and writing gzip and bzip2 files. They are transparently handled over HTTP, S3, and other protocols, too, based on the extension of the file being opened.

### Configuration

The Meltano configuration for this tap must contain the key 'tables' which holds an array of json objects describing each set of targeted source files.
```
config:
  extractors:
  - name: tap-spreadsheets-anywhere
    namespace: tap_spreadsheets_anywhere
    pip_url: git+https://github.com/ets/tap-spreadsheets-anywhere.git
    executable: tap-spreadsheets-anywhere
    capabilities:
    - catalog
    - discover
    - state
    config:
      tables: []
``` 

To run this tap directly from the CLI, a config.json file must be supplied which holds the 'tables' array.
A sample config file is available here [sample_config.json](sample_config.json) and a description of the required/optional fields declared within it follow.
The configuration is also captured in [tables_config_util.py](tap_spreadsheets_anywhere/configuration.py) as a [`voluptuous`](https://github.com/alecthomas/voluptuous)-based configuration for validation purposes.

```
{
    "tables": [
        {
            "path": "s3://my-s3-bucket",
            "name": "target_table_name",
            "pattern": "subfolder/common_prefix.*",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "csv",
            "delimiter": "|",
            "quotechar": '"',
            "universal_newlines": false,
            "skip_initial": 0,
            "sample_rate": 10,
            "max_sampling_read": 2000,
            "max_sampled_files": 3,
            "prefer_number_vs_integer": true,
            "prefer_schema_as_string": true,
            "selected": true,

            // for any field in the table, you can hardcode the json schema datatype to override
            // the schema infered through discovery mode. 
            // *Note Meltano users* - the scheam override support delivered in Meltano v1.41.1 is more robust
            //  and should be preferred to this tap-specific override functionality.  
            "schema_overrides": {
                "id": {
                    "type": ["null", "integer"],
                },
                // if you want the tap to enforce that a field is not nullable, you can do it like so:
                "first_name": {
                    "type": "string",
                }
            },
            "ignore_undefined_field_names": true,
            "ignore_state": true,
            "state_based_discovery": true
        },
        {
            "path": "sftp://username:password@host//path/file",
            "name": "another_table_name",
            "pattern": "subdir/.*User.*",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": ["id"],
            "format": "excel", 
            // you must specify the worksheet name to pull from in your xls(x) file.
            "worksheet_name": "Names"
        }
    ],
    "azure_storage_connection_string": "my_connection_string",
    "aws_access_key_id" : "my_access_key_id",
    "aws_secret_access_key" : "my_secret_access_key",
    "google_application_credentials" : "path/to/service_credentials.json",
    "google_application_credentials" : "{ \"project_id\": \"my_project_id\" }"
    "ssh_private_key": "-----BEGIN RSA PRIVATE KEY-----ahhhhhhhhhh..."
}

```
Each object in the 'tables' array describes one or more CSV or Excel spreadsheet files that adhere to the same schema and are meant to be tapped as the source for a Singer-based data flow.  
- **path**: A string describing the transport and bucket/root directory holding the targeted source files.
- **name**: A string describing the "table" (aka Singer stream) into which the source data should be loaded.
- **search_prefix**: (optional) This is an optional prefix to apply after the bucket that will be used to filter files in the listing request from the targeted system. This prefix potentially reduces the number of files returned from the listing request.
- **pattern**: This is an escaped regular expression that the tap will use to filter the listing result set returned from the listing request. This pattern potentially reduces the number of listed files that are considered as sources for the declared table. It's a bit strange, since this is an escaped string inside of an escaped string, any backslashes in the RegEx will need to be double-escaped.
- **start_date**: This is the datetime that the tap will use to filter files, based on the modified timestamp of the file.
- **key_properties**: These are the "primary keys" of the CSV files, to be used by the target for deduplication and primary key definitions downstream in the destination.
- **format**: Must be either 'csv', 'json', 'jsonl' ([JSON Lines](https://jsonlines.org/)), 'excel', or 'detect'. Note that csv can be further customized with delimiter and quotechar variables below.
- **invalid_format_action**: (optional) By default, the tap will raise an exception if a source file can not be read
. Set this key to "ignore" to skip such source files and continue the run.  
- **field_names**: (optional) An array holding the names of the columns in the targeted files. If not supplied, the first row of each file must hold the desired values. 
- **encoding**: (optional) The file encoding to use when reading text files (i.e., "utf-8" (default), "latin1", "windows-1252")
- **universal_newlines**: (optional) Should the source file parsers honor [universal newlines](https://docs.python.org/2.3/whatsnew/node7.html)). Setting this to false will instruct the parser to only consider '\n' as a valid newline identifier.
- **skip_initial**: (optional) How many lines should be skipped. The default is 0.
- **sample_rate**: (optional) The sampling rate to apply when reading a source file for sampling in discovery mode. A sampling rate of 1 will sample every line.  A sampling rate of 10 (the default) will sample every 10th line.
- **max_sampling_read**: (optional) How many lines of the source file should be sampled when in discovery mode attempting to infer a schema. The default is 1000 samples.
- **max_sampled_files**: (optional) The maximum number of files in the targeted set that will be sampled. The default is 5.
- **max_records_per_run**: (optional) The maximum number of records that should be written to this stream in a single sync run. The default is unlimited. 
- **prefer_number_vs_integer**: (optional) If the discovery mode sampling process sees only integer values for a field, should `number` be used anyway so that floats are not considered errors? The default is false but true can help in situations where floats only appear rarely in sources and may not be detected through discovery sampling.
- **prefer_schema_as_string**: (optional) Bool value either as true or false (default). Should the schema be all read as string by default.
- **selected**: (optional) Should this table be synced. Defaults to true. Setting to false will skip this table on a sync run.
- **worksheet_name**: (optional) the worksheet name to pull from in the targeted xls file(s). Only required when format is excel
- **delimiter**: (optional) the delimiter to use when format is 'csv'. Defaults to a comma ',' but you can set delimiter to 'detect' to leverage the csv "Sniffer" for auto-detecting delimiter. 
- **quotechar**: (optional) the character used to surround values that may contain delimiters - defaults to a double quote '"'
- **json_path**: (optional) the JSON key under which the list of objects to use is located. Defaults to None, corresponding to an array at the top level of the JSON tree.
- **ignore_undefined_field_names**: (optional) when enabled this removes all catalog entries where the field name is undefined (empty string), as these fields always cause errors with database targets. `Boolean` that defaults to `false`.
- **ignore_state**: (optional) when enabled this ignores the state for the specific table, this means we will default to getting all the applicable files from the **start_date** you have provided.
- **state_based_discovery**: (optional) when enabled makes discovery only happen to files found after the streams state. (Currently this only effects the IMAP protocol, paths starting `imap://`).

### Other Optional Tap Settings

- **azure_storage_connection_string**: (optional) the connection string to connect to and get files from Azure. This setting applies for all azure connections in your tables settings.

(To connect to multiple azure storages, you will need to have the tap run multiple times with different `azure_storage_connection_string` settings).

To obtain this setting:
1. Go to the Azure Portal in your browser, sign in if needed.
2. In the search bar look for "storage accounts".
3. Choose the storage account you want to connect to.
4. In the sidebar, click on `Access keys`.
5. Here you can find your connection string. (There is also a link to the docs about these keys, which will help if you need to create one).

---

- **aws_access_key_id** and **aws_secret_access_key**: (optional) these settings let you connect to your S3 bucket and sync spreadsheets from it.

(To connect to multiple S3 buckets, you will need to have the tap run multiple times with different `aws...` settings).

To obtain these settings we recommend following this link: [AWS Docs - Credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey)

---

- **google_application_credentials**: (optional) connect to Google Cloud Storage using a path to a credentials file, or the contents of a credentials file as JSON. See [`GOOGLE_APPLICATION_CREDENTIALS` environment variable](https://cloud.google.com/docs/authentication/application-default-credentials#GAC) for more information.

---

- **ssh_private_key**: (optional) connect to a SFTP server with a private key. Currently this key has to be an RSA key, and should include any spaces and newline characters in your keyfile. (Open to PRs to expand support to other key types).

---

### Replication methods and BATCH support

This tap only ever discovers and reads the full listing of matching files for each configured table on every run (filtered by `start_date`/state via each file's modified timestamp) - there is no `FULL_TABLE` vs `INCREMENTAL` distinction to configure, and no log-based replication.

By default, each row is emitted as its own Singer `RECORD` message. Setting the top-level `batch_config` key opts every stream in the run into Singer [BATCH](https://sdk.meltano.com) mode instead: rows are buffered and written out as [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) files, referenced from a `BATCH` message's `manifest`, which loaders that understand BATCH (e.g. target-postgres) can ingest far faster than row-by-row `RECORD`s.

```json
{
    "tables": [ ... ],
    "batch_config": {
        "encoding": { "format": "arrow" },
        "storage": { "root": "/path/to/a/writable/directory" },
        "batch_size": 100000
    }
}
```

- **encoding.format**: (optional) only `"arrow"` is currently supported. Other singer-sdk batch encodings (`jsonl`, `parquet`) are not implemented.
- **storage.root**: (optional) local directory batch files are written to. Defaults to the OS temp directory.
- **batch_size**: (optional) maximum number of rows buffered per Arrow IPC file before a `BATCH` message is emitted. Defaults to 100,000. A partial batch is still flushed (as its own file) at the end of each configured table.

Each Arrow column's type is derived from the same Singer JSON schema used for `RECORD` mode (`integer`→int64, `number`→float64, `boolean`→bool, everything else, including `date-time`-formatted strings and nested `object`/`array` values, as strings) - so switching `batch_config` on or off does not change the effective schema seen downstream.

### Automatic Config Generation

This is an experimental feature used to crawl a path and generate a config block for every file encountered. An intended 
use-case is where source files are organized in subdirectories by intended target table. This mode will generate a config
block for each subdirectory and for each file format within it. The following example config file will crawl the s3
bucket my-example-bucket and produce config blocks for each folder under it where source files are detected.

```
{
    "tables": [
        {
            "crawl_config": true,
            "path": "s3://my-example-bucket",
            "pattern": ".*"
        }
    ]
}
```  

Typically this mode will be used when there are many streams to be configured and processed. Therefore, generating the
 catalog independently is generally helpful.
```bash
meltano invoke --dump=catalog tap-spreadsheets-anywhere > my-catalog.json
meltano elt --catalog=my-catalog.json --job_id=my-job-state tap-spreadsheets-anywhere any-loader
``` 

### JSON support

JSON files are expected to parse as a root-level array of objects where each object is a set of flat key-value pairs.
```json
[
    { "name": "row one", "key": 42},
    { "name": "row two", "key": 43}
]
``` 

### JSONL (JSON Lines) support

JSONL files are expected to parse as one object per line, where each row in a file is a set of key-value pairs.
```jsonl
{ "name": "row one", "key": 42}
{ "name": "row two", "key": 43}
``` 

### Authentication and Credentials

This tap authenticates with target systems as described in the [smart_open documentation here](https://github.com/RaRe-Technologies/smart_open).

#### SharePoint

The tap reads SharePoint files with the Microsoft Graph API. It supports two OAuth 2.0 flows:

- The [client credentials flow](#client-credentials-flow). The tap authenticates as an application, with no user. Use this flow for a scheduled sync.
- The [refresh token flow](#refresh-token-flow). The tap authenticates as a user, with a refresh token that a person gets from an interactive sign-in.

The tap uses the client credentials flow when `oauth_credentials` holds no `refresh_token`.

##### Client credentials flow

Create the app registration under **App registrations** > **New registration**, in the Microsoft Entra admin center or in the **Microsoft Entra ID** section of the Azure portal. Use these values:

- **Supported account types**: **Accounts in this organizational directory only**, which is the single-tenant option.
- **Redirect URI**: leave this field empty. This flow has no sign-in, so it needs no redirect URI.
- **Certificates & secrets**: a client secret. Copy the **Value** column, and not the **Secret ID** column. The portal masks the value after you leave the page. Record the expiry date also, because the tap will fail when the secret expires.
- **API permissions**: the **Application permission** `Sites.Read.All` for Microsoft Graph, which lets the application read all sites of the tenant. Use `Sites.Selected` in place of `Sites.Read.All` to give the application access to named sites only, as [Restrict the access to named sites](#restrict-the-access-to-named-sites) describes. Then select **Grant admin consent for \<tenant\>**. The permission must show the **Granted** state. An application permission has no user consent, so a tenant administrator must grant the consent.

You can delete the `User.Read` permission that the portal adds to a new app registration. It is a delegated permission, and the tap does not use it.

Then configure the credentials of the app registration under `oauth_credentials`. The **Overview** page of the app registration shows the application (client) ID and the directory (tenant) ID:

```json
{
    "oauth_credentials": {
        "client_id": "<client_id>",
        "client_secret": "<client_secret>",
        "tenant_id": "<tenant_id>"
    }
}
```

| Setting | Required | Description |
| --- | --- | --- |
| `client_id` | Yes | The application (client) ID of the app registration. |
| `client_secret` | Yes | A client secret value of the app registration. |
| `tenant_id` | Yes | The directory (tenant) ID of the app registration. Give a tenant ID or a verified domain, such as `<tenant>.onmicrosoft.com`. |

If a tenant administrator does not grant admin consent, Microsoft Graph responds with `401 Unauthorized` and no explicit notice that consent is absent:

```json
{
    "error": {
        "code": "generalException",
        "message": "General exception while processing",
        "innerError": {
            "code": "spException",
            "innerError": {
                "code": "other"
            },
            "date": "<date>",
            "request-id": "<request_id>",
            "client-request-id": "<client_request_id>"
        }
    }
}
```

###### Restrict the access to named sites

The `Sites.Selected` application permission grants no access on consent. A SharePoint administrator then grants the application access to each site that the tap must read. This keeps the access to the sites that you select, and you can remove the access one site at a time.

The Microsoft Entra admin center and the Azure portal cannot grant a site to an application. Only the Microsoft Graph API can. Graph Explorer is a client for that API, so the steps below use Graph Explorer to make the requests.

These are the steps to grant the access:

1. In **API permissions**, remove `Sites.Read.All`. Add the **Application permission** `Sites.Selected` for Microsoft Graph. Then select **Grant admin consent for \<tenant\>**.

    **API permissions** holds the permissions that the application requests. It does not hold the permissions that the application has. Go to **Enterprise applications** also, select the application, and revoke `Sites.Read.All` under **Security** > **Permissions**. The application keeps the granted permission until you revoke it there, so a request that you expect to fail gives a `200 OK` response. **API permissions** lists such a permission under **Other permissions granted for \<tenant\>**.

    A change of permissions can take some minutes to apply. An access token also keeps the permissions that it had at issue time, for the full lifetime of the token.
2. Open [Graph Explorer](https://developer.microsoft.com/graph/graph-explorer), and sign in as a tenant administrator.

    Consent to `Sites.FullControl.All` on the **Modify permissions** tab. Graph Explorer makes the requests of step 3 and step 4 as itself, so Graph Explorer needs this permission to grant a site. The consent dialog must show **Consent on behalf of your organization**. An account without the Global Administrator, Privileged Role Administrator, or Cloud Application Administrator role sees the message **Need admin approval** instead, and cannot continue.

    Do not give `Sites.FullControl.All` to the app registration of the tap, even for a short time. This permission gives write access to every site of the tenant.
3. In Graph Explorer, get the ID of the site:

    ```http
    GET https://graph.microsoft.com/v1.0/sites/<host>:/sites/<site_name>
    ```

    `<host>` is the SharePoint host of the tenant, such as `<tenant>.sharepoint.com`. `<site_name>` is the last part of the URL of the site.

4. Grant the read role on that site to the application:

    ```http
    POST https://graph.microsoft.com/v1.0/sites/<site_id>/permissions

    {
        "roles": ["read"],
        "grantedToIdentities": [
            {
                "application": {
                    "id": "<client_id>",
                    "displayName": "<application_name>"
                }
            }
        ]
    }
    ```

    `id` must be the application (client) ID of the app registration. `displayName` is a label only, so any value works. Give the name of the app registration, to make the permission easy to recognise in a later request.

    The app registration of the tap cannot make this request for itself.

    Repeat for each site.

To remove the access to a site, get the ID of the site (as in step 3) and then list the permissions of that site:

```http
GET https://graph.microsoft.com/v1.0/sites/<site_id>/permissions
```

Each item of the response holds an `id`, and a `grantedToIdentitiesV2` value that names the application. Find the item for the app registration, and delete that permission:

```http
DELETE https://graph.microsoft.com/v1.0/sites/<site_id>/permissions/<permission_id>
```

##### Refresh token flow

Configure a refresh token under `oauth_credentials`, with either the credentials of the app registration or the refresh proxy settings:

```json
{
    "oauth_credentials": {
        "client_id": "<client_id>",
        "client_secret": "<client_secret>",
        "tenant_id": "<tenant_id>",
        "refresh_token": "<refresh_token>",
        "access_token": "<access_token>"
    }
}
```

| Setting | Required | Description |
| --- | --- | --- |
| `refresh_token` | Yes | A refresh token for a user of the tenant. |
| `client_id` | Yes, without a refresh proxy | The application (client) ID of the app registration. |
| `client_secret` | Yes, without a refresh proxy | A client secret value of the app registration. |
| `tenant_id` | No | The directory (tenant) ID of the app registration. The default value is `common`, which works for a multi-tenant app registration only. A single-tenant app registration rejects `common` with error `AADSTS50194`. |
| `refresh_proxy_url` | Yes, with a refresh proxy | The URL of the refresh proxy. |
| `refresh_proxy_url_auth` | Yes, with a refresh proxy | The value of the `Authorization` header for the refresh proxy request. |
| `access_token` | No | An access token. The tap gets a new access token with the refresh token when this setting is absent, or when the access token is not valid. |

If you set `refresh_proxy_url` and `refresh_proxy_url_auth`, `client_id` and `client_secret` are not necessary. The refresh proxy holds the credentials of the app registration, and keeps the refresh token current. This is the behaviour for an app registration that Meltano manages:

```json
{
    "oauth_credentials": {
        "refresh_token": "<refresh_token>",
        "refresh_proxy_url": "https://app.meltano.com/api/tokens/oauth-microsoft/token",
        "refresh_proxy_url_auth": "Bearer <token>"
    }
}
```

This flow uses delegated Microsoft Graph permissions, so the tap reads the content that the signed-in user can read, and no more. The user who completes the authorization flow therefore sets the upper limit of the access.

The tap does not persist the refresh token that Microsoft returns on each refresh. Microsoft keeps the configured refresh token valid for 90 days of inactivity. Microsoft also makes a refresh token not valid when the password of the user changes, when an administrator revokes the sessions of the user, or when a Conditional Access policy applies. Authorize the tap again when one of these events occurs, and before the inactivity period ends.

### State 

This tap is designed to continually poll a configured directory for any unprocessed files that match a table configuration and to process any 
that are found.  On the first syncing run, the declared start_date will be used to filter the set of files that match the search_prefix and pattern expressions. 
The last modified date of the most recently synced file will then be written to state and used in place of start_date on the next syncing run.

While state is maintained, only new files will be processed from subsequent runs. 

### Install and Run outside of Meltano

First, make sure Python 3 is installed on your system. Then, execute `create_virtualenv.sh` to create a local venv and install the necessary dependencies. If you are executing this tap outside of Meltano then you will need to supply the config.json file yourself. A sample configuration is available here [sample_config.json](sample_config.json)
You can invoke this tap directly with:
```
python -m tap_spreadsheets_anywhere --config config.json
```


---
History:
- this project borrowed heavily from [tap-s3-csv](https://github.com/singer-io/tap-s3-csv). That project was modified to use [smart_open](https://github.com/RaRe-Technologies/smart_open) for support beyond S3 and then migrated to the [cookie cutter based templates](https://github.com/singer-io/singer-tap-template) for taps. 
- Support for --discover was added so that target files could be sampled independent from sync runs
- CSV parsing was made more robust and support for configurable typing & sampling added
- The github commit log holds history from that point forward

Copyright &copy; 2020 Eric Simmerman
