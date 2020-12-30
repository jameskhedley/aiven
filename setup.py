from setuptools import setup
setup(
    name='aiven',
    install_requires=[
        'kafka-python>2', 'psycopg2', 'requests', 'pylint', 'coverage', 'mock'
    ]
)
