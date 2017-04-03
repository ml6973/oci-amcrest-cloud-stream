# OCI Amcrest Cloud Stream

Install Python 2.6 Dependencies:
```
$ sudo pip install amcrest
```

## Configuration
```
cd oci-amcrest-cloud-stream/configuration
vi config.txt
```

Set the appropriate fields in the configuration file.

## Usage
```
cd oci-amcrest-cloud-stream
python swiftstream.py
```

The program will stream the data to the cloud.

To stop streaming, press Ctrl + C and wait for the program to gracefully exit to ensure the stream finishes successfully.
