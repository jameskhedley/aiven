# Aiven
Trying out Aiven Kafka and PostgreSQL

### Introduction
This project is a demonstration of how one might write a system for monitoring websites using Kafka.

There are two main script files in this repo, `producer.py` and `consumer.py`. The former script does the job of querying a website, noting the observed response time, status code and searching for the given regex string, then publishing that information to kafka. The latter script listens to kafka and consumes messages, converting the information to SQL records which it inserts into PostgreSQL.

### Prerequisites
To use this project you will need to set up both Kafka and PostgreSQL instances at https://aiven.io/ - this is because I've written the connection handling code as per Aiven's SSL configuration particulars, although other service providers may do the same, I cannot guarantee that.

You will also need to download the following files for Kafka: `ca.pem`, `service.cert` and `service.key`, available from the service console. Aiven PostgeSQL is happy with only username and password (I probably should verify the ca cert!).

You must have Python 3.9 installed (it should work with older 3.x though). This should work with Windows, Linux and Mac.

### Installation
Checkout this repo to a convenient directory, cd into it and run the following command on Linux/Mac: 

```
$ python3 -m venv env
or
$ virtualenv env
$ source ./env/bin/activate
$ python3 -m pip install .
```

On Windows:

```
$ python3 -m venv env
$ .\env\Scripts\activate.bat
$ python3 -m pip install .
```

### Running the scripts

Each of `producer.py` and `consumer.py` can be run (`common.py` and `setup.py` cannot) but each has its own required command line arguments. To see these, run the desired script with `--help` e.g.:

```
$ python3 consumer.py --help
usage: consumer.py [-h] [--cert-path CERT_PATH] kafka_url topic_name postgesql_uri

Tool for monitoring websites using Aiven Kafka - consumer.

positional arguments:
  kafka_url             URL of the Aiven Kafka service to consume - required
  topic_name            Kafka topic to consume - required
  postgesql_uri         URI of the PostgreSQL instance - required

optional arguments:
  -h, --help            show this help message and exit
  --cert-path CERT_PATH
                        Directory containing access cert and key and CA cert for Aiven Kafka service
```

You should start 1 instance of `consumer.py` and n instances of `producer.py` (one for each page you want to monitor). You can start each `producer.py` in a new terminal if you want to see the output, or simply background them and pipe the output to files (all logging goes to stdout).

NB: When `consumer.py` is started, it will detect whether the necessary db table exists and if not, create it. So no manual setup is required aside from making sure the given database exists (it will not create a new db).

A concrete example of this would be (remember to use the virtualenv you set up during installation):

```
(env) $ python3 producer.py kafka-blah-account-blah.aivencloud.com:12345 mytopic https://www.bbc.co.uk/weather/2633858/day1  'Last updated<!-- --> <time>[^\s]+ at \d\d:\d\d</time>' --cert-path d:\certs 1>/tmp/log-1.txt &
(env) $ python3 producer.py kafka-blah-account-blah.aivencloud.com:12345 mytopic https://old.reddit.com/r/Python/  "Automate the Boring Stuff with Python" --cert-path d:\certs 1>/tmp/log-2.txt &
(env) $ python3 consumer.py kafka-blah-account-blah.aivencloud.com:12345 mytopic "postgres://user:password123@pg-blah-account-blah.aivencloud.com:11111/dbname?sslmode=require" --cert-path d:\certs
```

### Scaling up and monitoring lots of pages
For `producer.py`, you must supply a URL and a regex string to look for, each instance of `producer.py` only takes one pair of these. This is because I have envisioned that a likely use case would be containerisation, so you can spin up one instance of `producer.py` for each of the websites/pages you want to monitor. For example, 100 producer containers would scale rather better than 1 with 100 URLs to look for. The consumer container can presumably be scaled horizontally, for example in Kubernetes a deployment can be defined which can scale the producer up over however much hardware you want.

### Future enhancements
A good enhancement would be to implement a proper scheduler, so that instead of checking the target site every n seconds, the producer script might take a list of url/regex pairs and check each one of them according to a schedule, for example check 10 sites at 15 past each hour.

Another idea would be to handle db inserts by caching records in the consumer and having a threaded worker which does bulk inserts every 100 records or such. Right now there is one cursor execute + commit per record inserted, which is likely causing some overhead and might lag with heavy load.
