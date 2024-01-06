from setuptools import setup, find_packages

setup(
    name='medline',
    version='0.1.0',
    packages=find_packages(include=['medline', 'medline.*']),
    description='A custom Python package for medline xml processing',
    author='Daniel Wang',
    author_email='daniel.wangd5@gmail.com',
    install_requires=[
        # List of dependencies, e.g., 'numpy >= 1.18.1',
        'lxml==4.5.0',
        'Unidecode==1.1.1',
        'requests==2.23.0',
        'numpy==1.18.1',
        'beautifulsoup4==4.8.2',
        'six==1.14.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        # Other classifiers as needed
    ]
)
