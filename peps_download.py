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
import zipfile
import logging
from os.path import exists
from datetime import date, datetime


class OptionParser(optparse.OptionParser):

    def check_required(self, opt):
        option = self.get_option(opt)

        # Assumes the option's 'default' is set to None!
        if getattr(self.values, option.dest) is None:
            self.error("%s option not supplied" % option)


class GeoJSON:
    """GeoJSON class which allows to calculate bbox"""

    def __init__(self, gj_object):
        if gj_object['type'] == 'FeatureCollection':
            self.coords = list(self._flatten([f['geometry']['coordinates']
                                              for f in gj_object['features']]))
            self.features_count = len(gj_object['features'])
        elif gj_object['type'] == 'Feature':
            self.coords = list(self._flatten([
                gj_object['geometry']['coordinates']]))
            self.features_count = 1
        else:
            self.coords = list(self._flatten([gj_object['coordinates']]))
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


class ParserConfig:
    def __init__(self, config_path):
        config = parse_config(config_path)
        self.auth = config_path
        config = config['sentinel']

        # Set geometry with the order tile, geojson, bbox, and point
        self.tile = None
        self.geojson = None
        self.location = None
        self.lat = None
        self.lon = None
        self.latmin = None
        self.latmax = None
        self.lonmin = None
        self.lonmax = None
        if config['tile'] is not None:
            print("Use tile for query.")
            self.tile = config['tile']
        elif config['geojson'] is not None:
            print("Use geojson for query.")
            print("Warning: if the single feature is too big, only 500 imagery will be back.")
            print("Suggestion: use a small geojson each time.")
            self.geojson = config['geojson']
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
        if config['download_path'] is None:
            self.write_dir = '.'
        else:
            self.write_dir = config['download_path']
        self.collection = config['platformname']
        self.product_type = config['producttype']
        self.sensor_mode = config['sensoroperationalmode']
        self.no_download = not config['download']
        self.start_date = config['date_start']
        self.end_date = config['date_end']
        if config['clouds'] is None:
            self.clouds = 100
        else:
            self.clouds = config['clouds']
        self.windows = config['windows']
        self.extract = config['extract']
        self.search_json_file = config['catalog_json']
        self.sat = config['satellite']
        self.orbit = config['orbit']

        # Set logging
        if config['log_dir'] is not None:
            self.log = "{}/peps_download_{}.log"\
                .format(config['log_dir'],
                        datetime.now().strftime("%d%m%Y_%H%M"))
        else:
            self.log = "peps_download_{}.log" \
                .format(datetime.now().strftime("%d%m%Y_%H%M"))


def parse_config(auth_file_path):
    with open(auth_file_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config


def _query_catalog(options, query_geom, start_date, end_date, logger):
    # Parse catalog
    # If the query geom is a geojson with more than 1 feature
    if isinstance(query_geom, list):
        logger.info('Query based on geojson with multiple features.')
        json_file_tmp = 'tmp.json'
        json_all = {"type": "FeatureCollection",
                    "properties": {},
                    "features": []}
        for i in range(0, len(query_geom)):
            each = query_geom[i]
            latmin = each[1]
            latmax = each[3]
            lonmin = each[0]
            lonmax = each[2]
            query_geom_each = 'box={lonmin},{latmin},{lonmax},{latmax}' \
                .format(latmin=latmin, latmax=latmax,
                        lonmin=lonmin, lonmax=lonmax)
            if (options.product_type is None) and (options.sensor_mode is None):
                search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api/" \
                                 "collections/{}/search.json?{}\&startDate={}" \
                                 "\&completionDate={}\&maxRecords=500" \
                    .format(json_file_tmp, options.collection,
                            query_geom_each, start_date, end_date)
            else:
                product_type = "" if options.product_type is None else options.product_type
                sensor_mode = "" if options.sensor_mode is None else options.sensor_mode
                search_catalog = 'curl -k -o {} https://peps.cnes.fr/resto/api/' \
                                 'collections/{}/search.json?{}\&startDate={}' \
                                 '\&completionDate={}\&maxRecords=500' \
                                 '\&productType={}\&sensorMode={}' \
                    .format(json_file_tmp, options.collection,
                            query_geom_each, start_date, end_date,
                            product_type, sensor_mode)
            if options.windows:
                search_catalog = search_catalog.replace('\&', '^&')
            os.system(search_catalog)
            time.sleep(5)

            with open(json_file_tmp) as data_file:
                json_each = json.load(data_file)
                if 'ErrorCode' in json_each:
                    logger.error("Error in query of {}th feature: {}"
                                 .format(i, json_each['ErrorMessage']))
                else:
                    for n in range(0, len(json_each['features'])):
                        json_each['features'][n]['properties']['no_geom'] = i
                    json_all['features'].extend(json_each['features'])
            os.remove(json_file_tmp)

        # Write json_all as search_json_file
        with open(options.search_json_file, 'w') as f:
            json.dump(json_all, f)
        logger.info("Write gathered search json to {}.".format(options.search_json_file))

    # Regular condition
    else:
        logger.info("Query based on regular conditions.")
        if (options.product_type is None) and (options.sensor_mode is None):
            search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api/" \
                             "collections/{}/search.json?{}\&startDate={}" \
                             "\&completionDate={}\&maxRecords=500" \
                .format(options.search_json_file, options.collection,
                        query_geom, start_date, end_date)
        else:
            product_type = "" if options.product_type is None else options.product_type
            sensor_mode = "" if options.sensor_mode is None else options.sensor_mode
            search_catalog = 'curl -k -o {} https://peps.cnes.fr/resto/api/' \
                             'collections/{}/search.json?{}\&startDate={}' \
                             '\&completionDate={}\&maxRecords=500' \
                             '\&productType={}\&sensorMode={}' \
                .format(options.search_json_file, options.collection,
                        query_geom, start_date, end_date,
                        product_type, sensor_mode)

        if options.windows:
            search_catalog = search_catalog.replace('\&', '^&')

        logger.info(search_catalog)
        os.system(search_catalog)
        time.sleep(5)


def check_rename(tmpfile, options, prod, prodsize, logger):
    logger.info("{} {}".format(os.path.getsize(tmpfile), prodsize))
    if os.path.getsize(tmpfile) != prodsize:
        with open(tmpfile) as f_tmp:
            try:
                tmp_data = json.load(f_tmp)
                logger.warning("Result is a text file (might come from a wrong password file)")
                logger.info(tmp_data)
                sys.exit(-1)
            except ValueError:
                logger.warning("\nDownload was not complete, tmp file removed")
                os.remove(tmpfile)
                return

    zfile = "{}/{}.zip".format(options.write_dir, prod)
    os.rename(tmpfile, zfile)

    # Unzip file
    if options.extract and os.path.exists(zfile):
        try:
            with zipfile.ZipFile(zfile, 'r') as zf:
                safename = zf.namelist()[0].replace('/', '')
                zf.extractall(options.write_dir)
            safedir = os.path.join(options.write_dir, safename)
            if not os.path.isdir(safedir):
                raise Exception('Unzipped directory not found: ', zfile)

        except Exception as e:
            logger.warning(e)
            logger.warning('Could not unzip file: ' + zfile)
            os.remove(zfile)
            logger.warning('Zip file removed.')
            return
        else:
            logger.info('Product saved as : ' + safedir)
            os.remove(zfile)
            return
    logger.info("Product saved as : " + zfile)


def parse_catalog(options, logger):
    # Filter catalog result
    with open(options.search_json_file) as data_file:
        data = json.load(data_file)

    if 'ErrorCode' in data:
        logger.error(data['ErrorMessage'])
        sys.exit(-2)

    # Get unique features
    # Remove no_geom item
    try:
        for i in range(0, len(data["features"])):
            del data['features'][i]['properties']['no_geom']
    except:
        pass
    # Remove duplicates
    result = []
    for i in range(0, len(data['features'])):
        each = data['features'][i]
        if each not in result:
            result.append(each)
    data['features'] = result

    # Sort data
    download_dict = {}
    storage_dict = {}
    size_dict = {}
    if len(data["features"]) > 0:
        for i in range(len(data["features"])):
            prod = data["features"][i]["properties"]["productIdentifier"]
            feature_id = data["features"][i]["id"]
            try:
                storage = data["features"][i]["properties"]["storage"]["mode"]
                platform = data["features"][i]["properties"]["platform"]
                resourceSize = int(data["features"][i]["properties"]["resourceSize"])
                if storage == "unknown":
                    logger.error('Found a product with "unknown" status : %s' % prod)
                    logger.error("Product %s cannot be downloaded" % prod)
                    logger.error('Please send and email with product name to peps admin team : exppeps@cnes.fr')
                else:
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
                                size_dict[prod] = resourceSize
                        elif platform.startswith('S1'):
                            if relativeOrbit == options.orbit:
                                download_dict[prod] = feature_id
                                storage_dict[prod] = storage
                                size_dict[prod] = resourceSize
                    else:
                        download_dict[prod] = feature_id
                        storage_dict[prod] = storage
                        size_dict[prod] = resourceSize
            except:
                pass

        # cloud cover criteria:
        if options.collection[0:2] == 'S2':
            logger.info("Check cloud cover criteria.")
            for i in range(len(data["features"])):
                prod = data["features"][i]["properties"]["productIdentifier"]
                if data["features"][i]["properties"]["cloudCover"] > options.clouds:
                    del download_dict[prod], storage_dict[prod], size_dict[prod]

        # Selection of specific satellite
        if options.sat is not None:
            for i in range(len(data["features"])):
                prod = data["features"][i]["properties"]["productIdentifier"]
                if data["features"][i]["properties"]["platform"] != options.sat:
                    try:
                        del download_dict[prod], storage_dict[prod], size_dict[prod]
                    except KeyError:
                        pass

        for prod in download_dict.keys():
            logger.info("{} {}".format(prod, storage_dict[prod]))
    else:
        logger.warning("No product corresponds to selection criteria")
        sys.exit(-1)

    return prod, download_dict, storage_dict, size_dict


def peps_downloader(options):
    # Set up logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=options.log, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # Check download path
    if not exists(options.write_dir):
        os.mkdir(options.write_dir)

    # Initialize json file for searching
    if options.search_json_file is None or options.search_json_file == "":
        options.search_json_file = 'search.json'

    if options.sat is not None:
        logger.info("{} {}".format(options.sat, options.collection[0:2]))
        if not options.sat.startswith(options.collection[0:2]):
            print("Input parameters collection and satellite are incompatible")
            logger.error("Input parameters collection and satellite are incompatible")
            sys.exit(-1)

    # Define location for searching: location, point or rectangle
    if options.tile is None:
        if options.geojson is None:
            if options.location is None:
                if options.lat is None or options.lon is None:
                    if options.latmin is None or options.lonmin is None or \
                            options.latmax is None or options.lonmax is None:
                        print("Provide at least tile, location, coordinates, rectangle, or geojson")
                        logger.error("Provide at least tile, location, coordinates, rectangle, or geojson")
                        sys.exit(-1)
                    else:
                        geom = 'rectangle'
                else:
                    if options.latmin is None and options.lonmin is None and \
                            options.latmax is None and options.lonmax is None:
                        geom = 'point'
                    else:
                        print("Please choose between coordinates and rectangle, but not both")
                        logger.error("Please choose between coordinates and rectangle, but not both")
                        sys.exit(-1)
            else:
                if options.latmin is None and options.lonmin is None and \
                        options.latmax is None and options.lonmax is None and \
                        options.lat is None or options.lon is None:
                    geom = 'location'
                else:
                    print("Please choose location and coordinates, but not both")
                    logger.error("Please choose location and coordinates, but not both")
                    sys.exit(-1)
        else:
            if options.latmin is None and options.lonmin is None and \
                    options.latmax is None and options.lonmax is None and \
                    options.lat is None or options.lon is None and \
                    options.location is None:
                geom = 'geojson'
            else:
                print("Please choose location, coordinates, rectangle, or geojson, but not all")
                logger.error("Please choose location, coordinates, rectangle, or geojson, but not all")
                sys.exit(-1)

    # Generate query based on geometric parameters of catalog request
    if options.tile is not None:
        if options.tile.startswith('T') and len(options.tile) == 6:
            tileid = options.tile[1:6]
        elif len(options.tile) == 5:
            tileid = options.tile[0:5]
        else:
            print("Tile name is ill-formatted : 31TCJ or T31TCJ are allowed")
            logger.error("Tile name is ill-formatted : 31TCJ or T31TCJ are allowed")
            sys.exit(-4)
        query_geom = "tileid={}".format(tileid)
    elif geom == 'geojson':
        with open(options.geojson) as f:
            gj = geojson.load(f)
        if len(gj['features']) > 1:
            query_geom = list(map(lambda each: GeoJSON(each).bbox(), gj['features']))
        else:
            bbox_gj = GeoJSON(gj).bbox()
            latmin = bbox_gj[1]
            latmax = bbox_gj[3]
            lonmin = bbox_gj[0]
            lonmax = bbox_gj[2]
            query_geom = 'box={lonmin},{latmin},{lonmax},{latmax}'.format(
                latmin=latmin, latmax=latmax,
                lonmin=lonmin, lonmax=lonmax)
    elif geom == 'point':
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
        if options.start_date >= datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products after '2016-12-05' are stored in Tiled products collection")
            print("**** Please use option -c S2ST")
            logger.warning("Option -c S2ST should be used for sentinel-2 imagery after '2016-12-05'")
            time.sleep(5)
        elif options.end_date >= datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products after '2016-12-05' are stored in Tiled products collection")
            print("**** Please use option -c S2ST to get the products after that date")
            print("**** Products before that date will be downloaded")
            logger.warning("Option -c S2ST should be used for sentinel-2 imagery after '2016-12-05'. "
                           "Products before that date will be downloaded")
            time.sleep(5)

    if options.collection == 'S2ST':
        if options.end_date < datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products before '2016-12-05' are stored in non-tiled products collection")
            print("**** Please use option -c S2")
            logger.warning("Option -c S2 should be used for sentinel-2 imagery before '2016-12-05'")
            time.sleep(5)
        elif options.start_date < datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products before '2016-12-05' are stored in non-tiled products collection")
            print("**** Please use option -c S2 to get the products before that date")
            print("**** Products after that date will be downloaded")
            logger.warning("Option -c S2 should be used for sentinel-2 imagery before '2016-12-05'. "
                           "Products after that date will be downloaded")
            time.sleep(5)

    # ====================
    # read authentication file
    # ====================
    config = parse_config(options.auth)
    email = config['peps']['user']
    passwd = config['peps']['password']
    if email is None or passwd is None:
        print("Not valid email or passwd for peps.")
        logger.error("Not valid email or passwd for peps.")
        sys.exit(-1)

    # ====================
    # search in catalog
    # ====================
    # Clean search json file
    if os.path.exists(options.search_json_file):
        os.remove(options.search_json_file)

    # Parse catalog
    _query_catalog(options, query_geom, start_date, end_date, logger)

    # Read catalog
    prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)

    # ====================
    # Download
    # ====================

    if len(download_dict) == 0:
        logger.warning("No product matches the criteria")
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
                    logger.info("Stage tape product: {}".format(prod))
                    get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto/" \
                                  "collections/{}/{}/download" \
                                  "/?issuerId=peps &>/dev/null" \
                        .format(tmpfile, email, passwd,
                                options.collection, download_dict[prod])
                    os.system(get_product)
                    if os.path.exists(tmpfile):
                        os.remove(tmpfile)

        NbProdsToDownload = len(list(download_dict.keys()))
        logger.info("{}  products to download".format(NbProdsToDownload))
        while NbProdsToDownload > 0:
            # redo catalog search to update disk/tape status
            logger.info("Redo catalog search to update disk/tape status.")
            _query_catalog(options, query_geom, start_date, end_date, logger)
            prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)

            NbProdsToDownload = 0
            # download all products on disk
            for prod in list(download_dict.keys()):
                file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                              os.path.exists("{}/{}.zip".format(options.write_dir, prod))
                if not options.no_download and not file_exists:
                    if storage_dict[prod] == "disk":
                        tmticks = time.time()
                        tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                        logger.info("Download of product : {}".format(prod))
                        get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto" \
                                      "/collections/{}/{}/download" \
                                      "/?issuerId=peps" \
                            .format(tmpfile, email, passwd,
                                    options.collection, download_dict[prod])
                        # print(get_product)
                        os.system(get_product)
                        # check binary product, rename tmp file
                        if not os.path.exists("{}/tmp_{}.tmp".format(options.write_dir, tmticks)):
                            NbProdsToDownload += 1
                        else:
                            check_rename(tmpfile, options, prod, size_dict[prod], logger)

                elif file_exists:
                    logger.info("{} already exists".format(prod))

            # download all products on tape
            for prod in list(download_dict.keys()):
                file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                              os.path.exists("{}/{}.zip".format(options.write_dir, prod))
                if not options.no_download and not file_exists:
                    if storage_dict[prod] == "tape" or storage_dict[prod] == "staging":
                        NbProdsToDownload += 1

            if NbProdsToDownload > 0:
                logger.info("{} remaining products are on tape, let's wait 1 minutes before trying again"
                            .format(NbProdsToDownload))
                time.sleep(60)


# The function also could be called like this:
# options = ParserConfig('peps_config.yaml')
# peps_downloader(options)


# Main function for directly run the script
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
        print("example 6 : python {} -c S1 -p GRD -g 'study_area.geojson' -a peps.txt -d 2015-11-01 -f 2015-12-01"
              .format(prog))
        sys.exit(-1)
    else:
        # Add all options
        usage = "usage: %prog [options] "
        parser = OptionParser(usage=usage)

        parser.add_option("-l", "--location", dest="location", action="store", type="string",
                          help="town name (pick one which is not too frequent to avoid confusions)", default=None)
        parser.add_option("-g", "--geojson", dest="geojson", action="store", type="string",
                          help="the path of geojson file to query", default=None)
        parser.add_option("-a", "--auth", dest="auth", action="store", type="string",
                          help="Peps account and password yaml file")
        parser.add_option("-w", "--write_dir", dest="write_dir", action="store", type="string",
                          help="Path where the products should be downloaded", default='.')
        parser.add_option("-c", "--collection", dest="collection", action="store", type="choice",
                          help="Collection within theia collections", choices=['S1', 'S2', 'S2ST', 'S3'], default='S2')
        parser.add_option("-p", "--product_type", dest="product_type", action="store", type="string",
                          help="GRD, SLC, OCN (for S1) | S2MSI1C S2MSI2Ap (for S2)", default=None)
        parser.add_option("-m", "--sensor_mode", dest="sensor_mode", action="store", type="string",
                          help="EW, IW , SM, WV (for S1) | INS-NOBS, INS-RAW (for S2)", default=None)
        parser.add_option("-n", "--no_download", dest="no_download", action="store_true",
                          help="Do not download products, just print curl command", default=False)
        parser.add_option("-d", "--start_date", dest="start_date", action="store", type="string",
                          help="start date, fmt('2015-12-22')", default=None)
        parser.add_option("-t", "--tile", dest="tile", action="store", type="string",
                          help="Sentinel-2 tile number", default=None)
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
        parser.add_option("--windows", dest="windows", action="store_true",
                          help="For windows usage", default=False)
        parser.add_option("--cc", "--clouds", dest="clouds", action="store", type="int",
                          help="Maximum cloud coverage", default=100)
        parser.add_option("--sat", "--satellite", dest="sat", action="store", type="string",
                          help="S1A, S1B, S2A, S2B, S3A, S3B", default=None)
        parser.add_option("-x", "--extract", dest="extract", action="store_true",
                          help="Extract and remove zip file after download")
        parser.add_option("--ld", "--log_dir", dest="log_dir", action="store_true",
                          help="The path to save log file", default=None)
        (options, _) = parser.parse_args(args)

        # Set logging
        if options.log_dir is not None:
            options.log = "{}/peps_download_{}.log" \
                .format(options.log_dir,
                        datetime.now().strftime("%d%m%Y_%H%M"))
        else:
            options.log = "peps_download_{}.log" \
                .format(datetime.now().strftime("%d%m%Y_%H%M"))

        # Date format
        options.start_date = datetime.strptime(options.start_date, '%Y-%m-%d').date()
        options.end_date = datetime.strptime(options.end_date, '%Y-%m-%d').date()

        # Run
        peps_downloader(options)


if __name__ == '__main__':
    main(sys.argv[1:])
