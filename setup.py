# -*- coding: utf-8 -*-
from setuptools import setup

setup(name="cartowaze",
      author="Jorge Sanz <jsanz@carto.com>, Daniel Carrión <daniel@carto.com>",
      description="Connect Waze data sources with CARTO",
      version="0.0.2",
      license="MIT",
      url="https://github.com/CartoDB/carto-waze",
      install_requires=["carto>=1.3.0", "psycopg2>=2.7.5", "shapely>=1.6.4"],
      packages=["cartowaze", "cartowaze.backends"])
