try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
    from setuptools import setup, find_packages
                        
setup(
    name = "pythonsolr",
    version = "1.0",
    description = "Enhanced version of pysolr.",
    author = 'Andrew Montalenti',
    author_email = 'andrew@cogtree.com',
    maintainer = 'didier deshommes',
    maintainer_email = 'dfdeshom@gmail.com',
    packages=find_packages(exclude=['ez_setup']),
    classifiers = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Internet :: WWW/HTTP :: Indexing/Search'
    ],
    url = 'http://bitbucket.org/cogtree/python-solr/',
    install_requires = ['httplib2',]
    )
