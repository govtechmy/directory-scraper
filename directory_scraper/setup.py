from setuptools import setup, find_packages
import os

setup_dir = os.path.dirname(__file__)
requirements_path = os.path.join(setup_dir, '..', 'requirements.txt')

with open(requirements_path) as f:
    requirements = f.read().splitlines()

setup(
    name='directory_scraper',               # Name of your project
    version='0.1',                   # Version of your project
    packages=find_packages('src'),   # Look for packages in the 'src' directory
    package_dir={'': 'src'},         # Root package directory is 'src'
    install_requires=requirements,      # Set dependencies from requirements.txt
)