import argparse
from medline.MedlineUpdate import MedlineUpdate
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MEDLINE database update preprocessor: automatically download new contents')
    parser.add_argument('-d', '--dir', dest='update_dir', required=True,
                        help='update directory of the MEDLINE repository')

    args = parser.parse_args()
    ml = MedlineUpdate(args.update_dir)
    ml.download()
