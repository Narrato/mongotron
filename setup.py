#!/usr/bin/env python

import codecs
from setuptools import setup

setup(
	name='mongotron',
	version='0.3.2',
	description='Python mongodb ODM',
	long_description=codecs.open('README', "r", "utf-8").read(),
	author='Tony Million',
	author_email='tony@narrato.co',
	license='MIT License',
	url='http://www.github.com/narrato/mongotron',
	classifiers=[
		'Development Status :: 3 - Alpha',
		'Environment :: Other Environment',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: MIT License',
		'Operating System :: OS Independent',
		'Programming Language :: Python',
		'Topic :: Database',
		'Topic :: Utilities',
		'Topic :: Software Development :: Libraries :: Python Modules',
	],
	packages=['mongotron'],
	zip_safe=False,
	install_requires=[
		'pymongo>=2.4.1',
	]
)
