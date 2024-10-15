from setuptools import setup, find_packages

setup(
    name='directory_scraper',               # Name of your project
    version='0.1',                   # Version of your project
    packages=find_packages('src'),   # Look for packages in the 'src' directory
    package_dir={'': 'src'},         # Root package directory is 'src'
)

