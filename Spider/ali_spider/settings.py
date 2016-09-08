# -*- coding: utf-8 -*-

import configparser
import os
import sys

# File path
BASE_KEYWORDS_FILE = './config/base_keywords.txt'
EXTEND_KEYWORDS_FILE = './config/extend_keywords.txt'
NEGATIVE_KEYWORDS_FILE = './config/negative_keywords.txt'

# Database setting
DATABASE_URL = 'sqlite:///./database/data.db'

# Debug
DATABASE_ECHO = False
HTTP_DEBUGLEVEL = 0

# Login
LOGIN_ID = ''
LOGIN_PASSWORD = ''
LOGIN_TIMEOUT = 30
FIREFOX_PATH = '/usr/bin/firefox'

# Generate
REG_CATEGORIES = ''

_CONFIG_FILE = './config/config.ini'
_CONFIG_DICT = {
    'BASE_KEYWORDS_FILE': ['Files', 'base_keywords_file'],
    'EXTEND_KEYWORDS_FILE': ['Files', 'extend_keywords_file'],
    'NEGATIVE_KEYWORDS_FILE': ['Files', 'negative_keywords_file'],
    'DATABASE_URL': ['Database', 'database_url'],
    'DATABASE_ECHO': ['Debug', 'database_echo'],
    'HTTP_DEBUGLEVEL': ['Debug', 'http_debuglevel'],
    'LOGIN_ID': ['Login', 'login_id'],
    'LOGIN_PASSWORD': ['Login', 'login_password'],
    'LOGIN_TIMEOUT': ['Login', 'login_timeout'],
    'FIREFOX_PATH': ['Login', 'firefox_path'],
    'REG_CATEGORIES': ['Generate', 'reg_categories'],
}

def read_config():
    config = configparser.ConfigParser()
    config.read(_CONFIG_FILE)
    module = sys.modules[__name__]
    for key, value in _CONFIG_DICT.items():
        try:
            if key == 'DATABASE_ECHO':
                setattr(module, key, config.getboolean(value[0], value[1]))
            elif key in ['HTTP_DEBUGLEVEL', 'LOGIN_TIMEOUT']:
                setattr(module, key, config.getint(value[0], value[1]))
            else:
                setattr(module, key, config.get(value[0], value[1]))
        except (configparser.NoSectionError, configparser.NoOptionError):
            continue

def write_default_config():
    config = configparser.ConfigParser()
    os.makedirs(os.path.dirname(_CONFIG_FILE), exist_ok=True)
    module = sys.modules[__name__]
    with open(_CONFIG_FILE, 'w') as cfgfile:
        for key, value in _CONFIG_DICT.items():
            section = value[0]
            option = value[1]
            option_value = str(getattr(module, key)).lower()
            if not config.has_section(section):
                config.add_section(section)
            if not config.has_option(section, option):
                config.set(section, option, option_value)
        config.write(cfgfile)

if os.path.isfile(_CONFIG_FILE):
    read_config()
else:
    write_default_config()
