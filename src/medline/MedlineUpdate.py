import os
import argparse
import re
import ftplib
import glob
from conf import config
from dateutil import parser
import pymysql

class MedlineUpdate:
    """ MEDLINE database update """

    def __init__(self, update_dir):
        """Constructor
        update_dir: the directory for the update repository containing the xml.gz files in local server
        """
        self.update_dir = update_dir
        if not os.path.exists(self.update_dir):
            os.makedirs(self.update_dir)
        self.ftp = ftplib.FTP('ftp.ncbi.nlm.nih.gov')

    def ftp_login(self):
        self.ftp.login('anonymous', 'anonymous')

    def ftp_cwd(self):
        print('Checking remote MEDLINE update repository')
        subdir = 'pubmed/updatefiles'
        self.ftp.cwd(subdir)

    def download(self):
        """download the update files"""
        print('Begin downloading...')
        self.ftp_login()
        self.ftp_cwd()
        ftp_file_info = self.get_file_info()

        # get a listing of processed XML files
        existing_xmlgzs = set()
        for name in glob.glob(os.path.join(self.update_dir, "*.xml.gz")):
            existing_xmlgzs.add(os.path.basename(name))

        # get a listing of all xml.gz offering on FTP
        listed_xmlgzs = set()
        gz_pattern = re.compile('(\w+\.xml\.gz)')
        for fname in self.ftp.nlst():
            match = gz_pattern.search(fname)
            if match:
                listed_xmlgzs.add(match.group(1))

        new_xmlgzs = listed_xmlgzs - existing_xmlgzs

        # keep track of file list to update mysql metadata
        db = self.get_mysql_connection()
        cursor = db.cursor()

        ct = len(new_xmlgzs)
        if ct > 0:
            print('*** In total %d new entries are detected' % ct)
            for xmlgz in sorted(new_xmlgzs):
                xmlgz_path_local = os.path.join(self.update_dir, xmlgz)
                # in case of connection issue, keep trying
                while True:
                    try:
                        transfer_result = self.ftp.retrbinary('RETR ' + xmlgz, open(xmlgz_path_local, 'wb').write)
                        if transfer_result == '226 Transfer complete':
                            break
                    except ftplib.all_errors:
                        self.ftp_login()
                        self.ftp_cwd()

                print('%s downloaded' % xmlgz)

                # update mysql information
                print('Updating mysql metadata...')

                try:
                    print(ftp_file_info[xmlgz])
                    query = "insert into update_status values (%s, %s, %s)"
                    cursor.execute(query, ftp_file_info[xmlgz])
                    db.commit()
                except Exception as e:
                    db.rollback()
                    print(e)
                    print('Failed mysql metadata update')
                print('Finished mysql metadata update')
                print("----------------------------")

        else:
            print('No new entry detected')
        print('Finished downloading')
        db.close()

    def get_mysql_connection(self):
        """ update date information for all files """
        # read config from config.json file
        config_path = "conf/hive.config.json"
        config_set = config.read(config_path)
        db = pymysql.connect(config_set.get("mysql").get("host"),
                             config_set.get("mysql").get("user"),
                             config_set.get("mysql").get("password"),
                             config_set.get("mysql").get("database"))

        return db

    def get_file_info(self):
        """ unfortunately, there is a problem with the method. ftp.dir() cannot distinguish a time of 2019 vs 2020 """
        lines = []
        self.ftp.dir(".", lines.append)

        ftp_file_info = {}
        pattern = re.compile("\.xml\.gz$")
        for line in lines:
            tokens = line.split()
            name = tokens[8]
            if re.search(pattern, name):
                time_str = tokens[5] + " " + tokens[6]
                time = parser.parse(time_str)
                time_trimed_string = str(time).split()[0]
                ftp_file_info[name] = (name, time_trimed_string, False)
        return ftp_file_info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MEDLINE database update preprocessor: automatically download new contents')
    parser.add_argument('-d', '--dir', dest='update_dir', required=True,
                        help='update directory of the MEDLINE repository')

    args = parser.parse_args()
    ml = MedlineUpdate(args.update_dir)
    ml.download()

