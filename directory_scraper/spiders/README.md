### Spiders/ folder hierarchy

1. `ministry/`
   - Description: Contains spiders for scraping data directly from main government ministries.

2. `ministry_orgs/`
   - Description: Contains spiders for scraping agencies and departments under each ministry. Organized 'jabatan' and 'agensi' into subdirectories.

3. `non_ministry/`
   - Description: Contains spiders for scraping independent organizations, regulatory bodies, or major companies not tied to any ministry.

3. `bahagian_unit/`
   - Description: Contains spiders for scraping data related to descriptions and duties of various divisions or units within an organization.

Example:
```
|-- ministry/
|   |-- jpm.py
|   |-- mohr.py
|
|-- ministry_orgs/
|   |-- jpm/
|   |   |-- agensi/
|   |         |-- felda.py
|   |         |-- tabunghaji.py
|   |   |-- jabatan/
|   |          |-- nadma.py
|   |          |-- apm.py
|   |
|   |-- mohr/
|       |-- agensi/
|       |     |-- perkeso.py
|       |     |-- niosh.py
|       |-- jabatan/
|             |-- jtksabah.py
|
|-- non_ministry/
|     |-- banknegara.py
|     |-- securitiescommision.py
|     |-- yayasankhazanah.py
|     |-- petronas.py    
|
|-- bahagian_unit/
    |-- jpm/
    |   |-- jpm_bahagian.py/
    |-- mohr/
        |-- mohr_bahagian.py/
```