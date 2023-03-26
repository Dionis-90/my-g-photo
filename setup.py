from setuptools import setup, find_packages
setup(
    name='my-g-photo',
    version='2.0',
    author='Denys Shcherbyna',
    description='Downloads media files and metadata from your Google Photo storage to your local storage.',
    long_description='This is an application that gets, downloads media files and metadata from your Google Photo '
                     'storage to your local storage.',
    url='https://github.com:Dionis-90/my-g-photo',
    keywords='GooglePhoto',
    python_requires='>=3.6',
    packages=find_packages(include=['app.*', ]),
    install_requires=[
        'google_auth_oauthlib',
        'google-auth',
        'requests',
    ],
)
