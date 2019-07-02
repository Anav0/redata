from setuptools import setup

setup(
    name='redata',
    version='0.1',
    py_modules=['redata'],
    install_requires=[
        'Click',
        'pymongo'
    ],
    entry_points='''
        [console_scripts]
        redata=redata:main
    ''',
)
