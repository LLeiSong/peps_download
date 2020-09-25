#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
import json
import time
import os
import os.path
import optparse
import sys
import yaml
import geojson
from datetime import date


class OptionParser(optparse.OptionParser):

    def check_required(self, opt):
        option = self.get_option(opt)

        # Assumes the option's 'default' is set to None!
        if getattr(self.values, option.dest) is None:
            self.error("%s option not supplied" % option)


class GeoJSON:
    """GeoJSON class which allows to calculate bbox"""

    def __init__(self, geojson):
        if geojson['type'] == 'FeatureCollection':
            self.coords = list(self._flatten([f['geometry']['coordinates']
                           for f in geojson['features']]))
            self.features_count = len(geojson['features'])
        elif geojson['type'] == 'Feature':
            self.coords = list(self._flatten([
                        geojson['geometry']['coordinates']]))
            self.features_count = 1
        else:
            self.coords = list(self._flatten([geojson['coordinates']]))
            self.features_count = 1

    def _flatten(self, l):
        for val in l:
            if isinstance(val, list):
                for subval in self._flatten(val):
                    yield subval
            else:
                yield val

    def bbox(self):
        return [min(self.coords[::2]), min(self.coords[1::2]),
                max(self.coords[::2]), max(self.coords[1::2])]


def parse_config(auth_file_path):
    with open(auth_file_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config


def check_rename(tmpfile, options, prod):
    with open(tmpfile) as f_tmp:
        try:
            tmp_data = json.load(f_tmp)
            print("Result is a text file (might come from a wrong password file)")
            print(tmp_data)
            sys.exit(-1)
        except ValueError:
            pass

    os.rename("{}".format(tmpfile), "{}/{}.zip".format(options.write_dir, prod))
    print("product saved as : {}/{}.zip".format(options.write_dir, prod))


def parse_catalog(options):
    # Filter catalog result
    with open(options.search_json_file) as data_file:
        data = json.load(data_file)

    if 'ErrorCode' in data:
        print(data['ErrorMessage'])
        sys.exit(-2)

    # Sort data
    download_dict = {}
    storage_dict = {}
    for i in range(len(data["features"])):
        prod = data["features"][i]["properties"]["productIdentifier"]
        print(prod, data["features"][i]["properties"]["storage"]["mode"])
        feature_id = data["features"][i]["id"]
        try:
            storage = data["features"][i]["properties"]["storage"]["mode"]
            platform = data["features"][i]["properties"]["platform"]
            # parse the orbit number
            orbitN = data["features"][i]["properties"]["orbitNumber"]
            if platform == 'S1A':
                # calculate relative orbit for Sentinel 1A
                relativeOrbit = ((orbitN - 73) % 175) + 1
            elif platform == 'S1B':
                # calculate relative orbit for Sentinel 1B
                relativeOrbit = ((orbitN - 27) % 175) + 1

            if options.orbit is not None:
                if platform.startswith('S2'):
                    if prod.find("_R%03d" % options.orbit) > 0:
                        download_dict[prod] = feature_id
                        storage_dict[prod] = storage
                elif platform.startswith('S1'):
                    if relativeOrbit == options.orbit:
                        download_dict[prod] = feature_id
                        storage_dict[prod] = storage
            else:
                download_dict[prod] = feature_id
                storage_dict[prod] = storage
        except:
            pass
    return prod, download_dict, storage_dict


def peps_downloader(options):
    # Initialize json file for searching
    if options.search_json_file is None or options.search_json_file == "":
        options.search_json_file = 'search.json'

    # Define location for searching: location, point or rectangle
    if options.location is None:
        if options.lat is None or options.lon is None:
            if options.latmin is None or options.lonmin is None or \
                    options.latmax is None or options.lonmax is None:
                print("Provide at least a point or rectangle")
                sys.exit(-1)
            else:
                geom = 'rectangle'
        else:
            if options.latmin is None and options.lonmin is None and \
                    options.latmax is None and options.lonmax is None:
                geom = 'point'
            else:
                print("Please choose between point and rectangle, but not both")
                sys.exit(-1)

    else:
        if options.latmin is None and options.lonmin is None and \
                options.latmax is None and options.lonmax is None and \
                options.lat is None or options.lon is None:
            geom = 'location'
        else:
            print("Please choose location and coordinates, but not both")
            sys.exit(-1)

    # Generate query based on geometric parameters of catalog request
    if geom == 'point':
        query_geom = 'lat={}\&lon={}'.format(options.lat, options.lon)
    elif geom == 'rectangle':
        query_geom = 'box={lonmin},{latmin},{lonmax},{latmax}'.format(
            latmin=options.latmin, latmax=options.latmax,
            lonmin=options.lonmin, lonmax=options.lonmax)
    elif geom == 'location':
        query_geom = "q={}".format(options.location)

    # date parameters of catalog request
    if options.start_date is not None:
        start_date = options.start_date
        if options.end_date is not None:
            end_date = options.end_date
        else:
            end_date = date.today().isoformat()

    # special case for Sentinel-2
    if options.collection == 'S2':
        if options.start_date >= '2016-12-05':
            print("**** Products after '2016-12-05' are stored in Tiled products collection")
            print("**** Please use option -c S2ST")
            time.sleep(5)
        elif options.end_date >= '2016-12-05':
            print("**** Products after '2016-12-05' are stored in Tiled products collection")
            print("**** Please use option -c S2ST to get the products after that date")
            print("**** Products before that date will be downloaded")
            time.sleep(5)

    if options.collection == 'S2ST':
        if options.end_date < '2016-12-05':
            print("**** Products before '2016-12-05' are stored in non-tiled products collection")
            print("**** Please use option -c S2")
            time.sleep(5)
        elif options.start_date < '2016-12-05':
            print("**** Products before '2016-12-05' are stored in non-tiled products collection")
            print("**** Please use option -c S2 to get the products before that date")
            print("**** Products after that date will be downloaded")
            time.sleep(5)

    # ====================
    # read authentication file
    # ====================
    config = parse_config(options.auth)
    email = config['peps']['user']
    passwd = config['peps']['password']

    # ====================
    # search in catalog
    # ====================
    # Clean search json file
    if os.path.exists(options.search_json_file):
        os.remove(options.search_json_file)

    # Parse catalog
    if (options.product_type == "") and (options.sensor_mode == ""):
        search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api/" \
                         "collections/{}/search.json?{}\&startDate={}" \
                         "\&completionDate={}\&maxRecords=500"\
            .format(options.search_json_file, options.collection,
                    query_geom, start_date, end_date)
    else:
        search_catalog = 'curl -k -o {} https://peps.cnes.fr/resto/api/' \
                         'collections/{}/search.json?{}\&startDate={}' \
                         '\&completionDate={}\&maxRecords=500' \
                         '\&productType={}\&sensorMode={}' \
            .format(options.search_json_file, options.collection,
                    query_geom, start_date, end_date,
                    options.product_type, options.sensor_mode)

    print(search_catalog)
    os.system(search_catalog)
    time.sleep(5)

    # Read catalog
    prod, download_dict, storage_dict = parse_catalog(options)

    # ====================
    # Download
    # ====================

    if len(download_dict) == 0:
        print("No product matches the criteria")
    else:
        # first try for the products on tape
        if options.write_dir is None:
            options.write_dir = os.getcwd()

        for prod in list(download_dict.keys()):
            file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                          os.path.exists("{}/{}.zip".format(options.write_dir, prod))
            if not options.no_download and not file_exists:
                if storage_dict[prod] == "tape":
                    tmticks = time.time()
                    tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                    print("\nStage tape product: {}".format(prod))
                    get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto/" \
                                  "collections/{}/{}/download" \
                                  "/?issuerId=peps &>/dev/null" \
                        .format(tmpfile, email, passwd,
                                options.collection, download_dict[prod])
                    os.system(get_product)

        NbProdsToDownload = len(list(download_dict.keys()))
        print("##########################")
        print("{}  products to download".format(NbProdsToDownload))
        print("##########################")
        while NbProdsToDownload > 0:
            # redo catalog search to update disk/tape status
            if (options.product_type == "") and (options.sensor_mode == ""):
                search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api" \
                                 "/collections/{}/search.json?{}" \
                                 "\&startDate={}\&completionDate={}\&maxRecords=500" \
                    .format(options.search_json_file, options.collection,
                            query_geom, start_date, end_date)
            else:
                search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api" \
                                 "/collections/{}/search.json?{}" \
                                 "\&startDate={}\&completionDate={}" \
                                 "\&maxRecords=500\&productType={}\&sensorMode={}" \
                    .format(options.search_json_file, options.collection,
                            query_geom, start_date, end_date,
                            options.product_type, options.sensor_mode)

            os.system(search_catalog)
            time.sleep(2)

            prod, download_dict, storage_dict = parse_catalog(options)

            NbProdsToDownload = 0
            # download all products on disk
            for prod in list(download_dict.keys()):
                file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                              os.path.exists("{}/{}.zip".format(options.write_dir, prod))
                if not options.no_download and not file_exists:
                    if storage_dict[prod] == "disk":
                        tmticks = time.time()
                        tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                        print("\nDownload of product : {}".format(prod))
                        get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto" \
                                      "/collections/{}/{}/download" \
                                      "/?issuerId=peps" \
                            .format(tmpfile, email, passwd,
                                    options.collection, download_dict[prod])
                        print(get_product)
                        os.system(get_product)
                        # check binary product, rename tmp file
                        if not os.path.exists("{}/tmp_{}.tmp".format(options.write_dir, tmticks)):
                            NbProdsToDownload += 1
                        else:
                            check_rename(tmpfile, options, prod)

                elif file_exists:
                    print("{} already exists" % prod)

            # download all products on tape
            for prod in list(download_dict.keys()):
                file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                              os.path.exists("{}/{}.zip".format(options.write_dir, prod))
                if not options.no_download and not file_exists:
                    if storage_dict[prod] == "tape":
                        tmticks = time.time()
                        tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                        print("\nDownload of product : {}" % prod)
                        get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/" \
                                      "resto/collections/{}/{}/download" \
                                      "/?issuerId=peps" \
                            .format(tmpfile, email, passwd,
                                    options.collection, download_dict[prod])
                        print(get_product)
                        os.system(get_product)
                        if not os.path.exists("{}/tmp_{}.tmp".format(options.write_dir, tmticks)):
                            NbProdsToDownload += 1
                        else:
                            check_rename(tmpfile, options, prod)

            if NbProdsToDownload > 0:
                print("##############################################################################")
                print("{} remaining products are on tape, let's wait 2 minutes before trying again"
                      .format(NbProdsToDownload))
                print("##############################################################################")
                time.sleep(120)


class ParserConfig:
    def __init__(self, config_path):
        config = parse_config(config_path)
        self.auth = config_path
        config = config['sentinel']

        # Set geometry
        self.location = None
        self.lat = None
        self.lon = None
        self.latmin = None
        self.latmax = None
        self.lonmin = None
        self.lonmax = None
        if config['geojson'] is not None:
            print("Geojson is set, so just use it for query.")
            print("Warning: if the geojson is too big, only 500 imagery will be back.")
            print("Suggestion: use a small geojson each time.")
            with open(config['geojson']) as f:
                gj = geojson.load(f)
            bbox_gj = GeoJSON(gj).bbox()
            self.latmin = bbox_gj[1]
            self.latmax = bbox_gj[3]
            self.lonmin = bbox_gj[0]
            self.lonmax = bbox_gj[2]

        elif config['bbox'] is not None:
            print("Use bbox to query.")
            self.latmin = config['bbox'][0]
            self.latmax = config['bbox'][1]
            self.lonmin = config['bbox'][2]
            self.lonmax = config['bbox'][3]
        elif config['point'] is not None:
            print("Use coordinate to query.")
            self.lon = config['point'][0]
            self.lat = config['point'][1]
        elif config['location'] is not None:
            print("Use location to query.")
            self.location = config['location']

        # Set sentinel parameter
        self.write_dir = config['download_path']
        self.collection = config['platformname']
        self.product_type = config['producttype']
        self.sensor_mode = config['sensoroperationalmode']
        self.no_download = not config['download']
        self.start_date = config['date_start']
        self.end_date = config['date_end']
        self.json = config['catalog_json']

# The function also could be called like this:
# options = ParserConfig('peps_config.yaml')
# peps_downloader(options)


def main(args):
    # ====================
    # Parse command line
    # ====================
    # Provide some examples
    if len(args) == 0:
        prog = os.path.basename(__file__)
        print(prog + ' [options]')
        print("Help: ", prog, " --help")
        print("Or: ", prog, " -h")
        print("example 1 : python {} -l 'Toulouse' -a peps.txt -d 2016-12-06 -f 2017-02-01 -c S2ST".format(prog))
        print(
            "example 2 : python {} --lon 1 --lat 44 -a peps.txt -d 2015-11-01 -f 2015-12-01 -c S2".format(prog))
        print("example 3 : python {} --lonmin 1 --lonmax 2 --latmin 43 --latmax 44 -a peps.txt -d 2015-11-01 -f "
              "2015-12-01 -c S2".format(prog))
        print("example 4 : python {} -l 'Toulouse' -a peps.txt -c SpotWorldHeritage -p SPOT4 -d 2005-11-01 -f "
              "2006-12-01".format(prog))
        print("example 5 : python {} -c S1 -p GRD -l 'Toulouse' -a peps.txt -d 2015-11-01 -f 2015-12-01"
              .format(prog))
        sys.exit(-1)
    else:
        # Add all options
        usage = "usage: %prog [options] "
        parser = OptionParser(usage=usage)

        parser.add_option("-l", "--location", dest="location", action="store", type="string",
                          help="town name (pick one which is not too frequent to avoid confusions)", default=None)
        parser.add_option("-a", "--auth", dest="auth", action="store", type="string",
                          help="Peps account and password yaml file")
        parser.add_option("-w", "--write_dir", dest="write_dir", action="store", type="string",
                          help="Path where the products should be downloaded", default='.')
        parser.add_option("-c", "--collection", dest="collection", action="store", type="choice",
                          help="Collection within theia collections", choices=['S1', 'S2', 'S2ST', 'S3'], default='S2')
        parser.add_option("-p", "--product_type", dest="product_type", action="store", type="string",
                          help="GRD, SLC, OCN (for S1) | S2MSI1C S2MSI2Ap (for S2)", default="")
        parser.add_option("-m", "--sensor_mode", dest="sensor_mode", action="store", type="string",
                          help="EW, IW , SM, WV (for S1) | INS-NOBS, INS-RAW (for S2)", default="")
        parser.add_option("-n", "--no_download", dest="no_download", action="store_true",
                          help="Do not download products, just print curl command", default=False)
        parser.add_option("-d", "--start_date", dest="start_date", action="store", type="string",
                          help="start date, fmt('2015-12-22')", default=None)
        parser.add_option("--lat", dest="lat", action="store", type="float",
                          help="latitude in decimal degrees", default=None)
        parser.add_option("--lon", dest="lon", action="store", type="float",
                          help="longitude in decimal degrees", default=None)
        parser.add_option("--latmin", dest="latmin", action="store", type="float",
                          help="min latitude in decimal degrees", default=None)
        parser.add_option("--latmax", dest="latmax", action="store", type="float",
                          help="max latitude in decimal degrees", default=None)
        parser.add_option("--lonmin", dest="lonmin", action="store", type="float",
                          help="min longitude in decimal degrees", default=None)
        parser.add_option("--lonmax", dest="lonmax", action="store", type="float",
                          help="max longitude in decimal degrees", default=None)
        parser.add_option("-o", "--orbit", dest="orbit", action="store", type="int",
                          help="Orbit Path number", default=None)
        parser.add_option("-f", "--end_date", dest="end_date", action="store", type="string",
                          help="end date, fmt('2015-12-23')", default='9999-01-01')
        parser.add_option("--json", dest="search_json_file", action="store", type="string",
                          help="Output search JSON filename", default=None)
        (options, _) = parser.parse_args(args)
        peps_downloader(options)


if __name__ == '__main__':
    main(sys.argv[1:])
