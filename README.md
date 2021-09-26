## Introduction

`port2viz` is a [Python](https://www.python.org/) based web scraping/data mining tool for UN/LOCODE (Code for Trade and Transport Locations). It helps you to harvest data from various page sources, format it and generate useful information for a maritime related industry operations.

## Prerequisites

- Download and install `Python 3`.
- Clone the repository by either calling git clone https://github.com/generomuga/port2viz in the terminal OR clicking the clone repository button above.
- Setup a virtual environment using `virtualenv` package (https://uoa-eresearch.github.io/eresearch-cookbook/recipe/2014/11/26/python-virtual-env/)
- Activate virtual environment and install required dependencies.

## Dependencies

This tool requires the following packages installed for execution and future development:

- beautifulsoup4==4.10.0
- certifi==2021.5.30
- charset-normalizer==2.0.6
- et-xmlfile==1.1.0
- html5lib==1.1
- idna==3.2
- lxml==4.6.3
- numpy==1.21.2
- openpyxl==3.0.9
- pandas==1.3.3
- python-crontab==2.5.1
- python-dateutil==2.8.2
- pytz==2021.1
- requests==2.26.0
- reverse-geocoder==1.5.1
- schedule==1.1.0
- scipy==1.7.1
- six==1.16.0
- soupsieve==2.2.1
- urllib3==1.26.7
- webencodings==0.5.1

To install these packages easily, you can follow these steps:

1. Open command prompt/terminal.
2. Change directory (cd) to the directory where your `requirements.txt` is located.
3. Type `python -m pip install -r requirements.txt`.

## File Structure

- config (contains the operation configurations)
- db (contains the raw and output databases)
- export (contains the failed mapping excel file)
- lib (contains Python classes)
- logs (contains the logging information)
- scripts (contains the sql scripts)
- controller.py (contains scheduler function)
- main.py (contains scraping operations)
- test.py (contains unit test)
- requirements.txt (contains dependencies)

## About the Developer

This tool was developed by [Gene Romuga](https://github.com/generomuga), a programmer and constant student of IT.
