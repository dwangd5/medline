import json

config_set = {}


def read(config_path):
    """main entry point, load and validate config and call generate"""
    try:
        with open(config_path) as handle:
            config = json.load(handle)
        return config

    except IOError as error:
        print("Error opening config file '%s'" % config_path, error)


if __name__ == '__main__':
    read("config.json")
