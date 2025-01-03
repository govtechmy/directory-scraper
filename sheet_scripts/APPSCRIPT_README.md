# Apps Script Deployment

## Overview
The goal of this tool is to manage the data scraped from the spiders in `/directory_scraper/spiders`. It creates an Google Sheets bound Apps Script webapp to validate the data and make it easy to edit the data.


## Getting started
Follow the instructions below to set up the environment and run the Apps Script deployer.

Make sure you have the following installed:
- bash 5.x or above ([macos](https://medium.com/@sisiliang/watch-out-these-details-when-upgrading-to-bash-5-on-mac-m1-m2-56bcabcfc549), [WSL/Linux](https://linux.die.net/man/8/apt-get))
- [clasp](https://www.npmjs.com/package/@google/clasp#deploy)


## Installation
1. Clone the repository to your local machine
```
git clone <repository-url>
cd <repository-directory>
```

2. Install bash 5.x on your local machine


## Runing Setup
- 
- Fill in sheet names in `sheet_scripts/sheetconfig.sh` file
- Run setup with `bash sheet_scripts/gas_ministry_setup.sh`