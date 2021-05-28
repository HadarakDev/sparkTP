from setuptools import setup, find_packages
setup(
 name="FootballApp",
 version="0.1",

 # Project uses reStructuredText, so ensure that the docutils get
 # installed or upgraded on the target machine
 install_requires=["setuptools==52.0.0", "pyspark==3.1.1", "pytest==6.2.4"],
 package_data={
 },
 packages = find_packages(exclude=['*tests*']),
 # metadata to display on PyPI
 author="NicolasRoche",
 author_email="nico_roche@hotmail.fr",
 description="This is an exercice Package",
 # could also include long_description, download_url, etc.
)