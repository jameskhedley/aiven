from setuptools import setup
setup(
    name='aiven',
    packages=['consumer', 'producer', 'common'],
    install_requires=[
        'kafka-python>2', 'psycopg2', 'requests'
    ]
)