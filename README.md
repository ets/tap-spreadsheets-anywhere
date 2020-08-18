# tap-spreadsheets-anywhere

This is a [Singer](https://singer.io) tap that reads data from files accessible from any [smart_open](https://github.com/RaRe-Technologies/smart_open) supported transport and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md). This tap is developed for compatibility with [Meltano](https://meltano.com/).

## How to use it

`tap-spreadsheets-anywhere` works together with any other [Singer Target](https://singer.io) to move data from any [smart_open](https://github.com/RaRe-Technologies/smart_open) supported transport to any target destination. [smart_open](https://github.com/RaRe-Technologies/smart_open) supports a wide range of transport options out of the box, including:

- S3
- local directories (file://)
- HTTP, HTTPS (read-only)
- SSH, SCP and SFTP
- WebHDFS
- GCS
- Azure Blob Storage

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
The configuration is also captured in [tables_config_util.py](tap_spreadsheets_anywhere/tables_config_util.py) as a [`voluptuous`](https://github.com/alecthomas/voluptuous)-based configuration for validation purposes.

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
            "sample_rate": 10,
            "max_sampling_read": 2000,
            "max_sampled_files": 3,
            "prefer_number_vs_integer": true,
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
            }
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
    ]
}

```
Each object in the 'tables' array describes one or more CSV or Excel spreadsheet files that adhere to the same schema and are meant to be tapped as the source for a Singer-based data flow.  
- **path**: A string describing the transport and bucket/root directory holding the targeted source files.
- **name**: A string describing the "table" into which the source data should be loaded.
- **search_prefix**: (optional) This is an optional prefix to apply after the bucket that will be used to filter files in the listing request from the targeted system. This prefix potentially reduces the number of files returned from the listing request.
- **pattern**: This is an escaped regular expression that the tap will use to filter the listing result set returned from the listing request. This pattern potentially reduces the number of listed files that are considered as sources for the declared table. It's a bit strange, since this is an escaped string inside of an escaped string, any backslashes in the RegEx will need to be double-escaped.
- **start_date**: This is the datetime that the tap will use to filter files, based on the modified timestamp of the file.
- **key_properties**: These are the "primary keys" of the CSV files, to be used by the target for deduplication and primary key definitions downstream in the destination.
- **format**: Must be either 'csv' or 'excel'
- **field_names**: (optional) An array holding the names of the columns in the targeted files. If not supplied, the first row of each file must hold the desired values. 
- **universal_newlines**: (optional) Should the source file parsers honor [universal newlines](https://docs.python.org/2.3/whatsnew/node7.html)). Setting this to false will instruct the parser to only consider '\n' as a valid newline identifier.
- **sample_rate**: (optional) The sampling rate to apply when reading a source file for sampling in discovery mode. A sampling rate of 1 will sample every line.  A sampling rate of 10 (the default) will sample every 10th line.
- **max_sampling_read**: (optional) How many lines of the source file should be sampled when in discovery mode attempting to infer a schema. The default is 1000 samples.
- **max_sampled_files**: (optional) The maximum number of files in the targeted set that will be sampled. The default is 5.
- **prefer_number_vs_integer**: (optional) If the discovery mode sampling process sees only integer values for a field, should `number` be used anyway so that floats are not considered errors? The default is false but true can help in situations where floats only appear rarely in sources and may not be detected through discovery sampling.
ies to your files.
- **selected**: (optional) Should this table be synced. Defaults to true. Setting to false will skip this table on a sync run.
- **worksheet_name**: (optional) the worksheet name to pull from in the targeted xls file(s). Only required when format is excel
- **delimiter**: (optional) the delimiter to use when format is 'csv' - defaults to a comma ','
- **quotechar**: (optional) the character used to surround values that may contain delimiters - defaults to a double quote '"'

### Authentication and Credentials

This tap authenticates with target systems as described in the [smart_open documentation here](https://github.com/RaRe-Technologies/smart_open).


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
