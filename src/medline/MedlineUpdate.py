import os
import argparse
import re
import ftplib
import glob


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

        else:
            print('No new entry detected')
        print('Finished downloading')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MEDLINE database update preprocessor: automatically download new contents')
    parser.add_argument('-d', '--dir', dest='update_dir', required=True,
                        help='update directory of the MEDLINE repository')

    args = parser.parse_args()
    ml = MedlineUpdate(args.update_dir)
    ml.download()

