## City Data Portal Conversion

### This repo is testing only so far. Trying to clean up KML data out of the City of Sacramento Data Portal.

### What files do what:
1. xml2csv.py: html convert to CSV. Extracts tables.
2. kml2csv.py: kml convert to CSV. Does not do much yet.
3. jrcsv2gncsv.py  converts from the klm like hierarchical csv format used by junar open data platform 
to a flat csv format. The script reads in a junar csv and emits a standard csv, paths given by fnamein and fnameout
Code tested with Sacramento Cites locations-of-city-trees.csv and PARKI-SPACE.csv

