# Directory Scraper

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

4. Install the required dependencies by running:
    ```bash
    pip install -r requirements.txt
    ```

### Running the Spider

To run an individual spider & produce an output, use the following command:

```bash
scrapy crawl <spider_name> -o result.json
```

Example:
```bash
scrapy crawl mof -o output.json
```
