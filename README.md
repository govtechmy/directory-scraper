# Directory Scraper

## Overview

This project is a web scraping tool built using Scrapy, designed to scrape directory listings and save the results in JSON format. It uses spiders to collect specific information from websites and outputs the data in a structured format.

## Getting Started

Follow the instructions below to set up the environment and run the scraper.

Make sure you have the following installed:
- **Python 3.11**

### Installation

1. Clone the repository to your local machine.
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2. Create a Python virtual environment using Python 3.11.
    ```bash
    python3.11 -m venv env
    ```

3. Activate the virtual environment.

    - On macOS/Linux:
      ```bash
      source env/bin/activate
      ```

    - On Windows:
      ```bash
      env\Scripts\activate
      ```
==============================================================

4a. Install the required dependencies by running:
    ```bash
    pip install -r requirements.txt
    ```
Alternatively, ignore step 4a, and run step 4b instead (suggested):

4b. Install the project setup.py 
The project is organized as a Python package. 

To install it in editable mode (development):
- -e (editable mode): This allows you to make changes to the project source code, and those changes will be immediately reflected without needing to reinstall the package.
    ```bash
    pip install -e .
    ```
To install it in non-editable mode
    ```bash
    pip install .
    ```  

You will be able to install dependencies, and import the project’s internal modules (e.g utils.file_utils) without needing to modify sys.path.

==============================================================

### Running the Spider

To run an individual spider & produce an output, use the following command:

```bash
scrapy crawl <spider_name> -o result.json
```

Example:
```bash
scrapy crawl mof -o output.json
```

-------------

## Installation of Tesseract OCR

To run this project, you will need to have Tesseract OCR installed on your system. Specifically, for running:
- spiders/kpkt.py

### 1. macOS

#### Steps:
1. Install Tesseract via **Homebrew** :
   ```bash
   brew install tesseract
   ```
2. Verify the installation by running:
   ```bash
   tesseract --version
   ```
3. Check path (this will be the path to .env TESSERACT_PATH):
   ```bash
   which tesseract
   ```

-------------

## Building Docker Image and Running Container

To build and run the API, you will need to download install Docker and Docker Compose.

### 1. macOS

#### Steps:
1.  Install Docker and Docker Compose

2.  Navigate to the root folder of the repo

3.  Build the container by running:
    ```
    docker compose -f docker/docker-compose.yml build --no-cache
    ```

4.  Run the container using:
    ```
    docker compose -f docker/docker-compose.yml up
    ```
    
    To run in detached mode use:
    ```
    docker compose -f docker/docker-compose.yml up --detach
    ```

5.  Access the API by going to: `http://0.0.0.0:80`

    or to access the interactive docs, go to: `http://0.0.0.0:80/docs`

5.  To shut down the container run:
    ```
    docker compose -f docker/docker-compose.yml down
    ```