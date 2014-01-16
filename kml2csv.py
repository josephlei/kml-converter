import json
import sys, os
from xml.etree import ElementTree as et
from bs4 import BeautifulSoup

NS = {'kml': 'http://www.opengis.net/kml/2.2'}

def main():
    kml_file = sys.argv[1]
    print "Opening kml file: " + kml_file
    tree = et.parse(kml_file)
    root = tree.getroot()

    get_pms(root)

####

def get_pms(doc):
    print "Entering GetPlacemarks"

    for plcmrk in doc.findall('./kml:Document/kml:Folder/kml:Placemark', NS):
        id_ = plcmrk.get('id')

        print plcmrk

####

main()

