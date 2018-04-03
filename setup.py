from distutils.core import setup

setup(name='cvangysel',
      version='0.2',
      description='Cross-project and reusable code',
      author='Christophe Van Gysel',
      author_email='cvangysel@uva.nl',
      packages=['cvangysel'],
      package_dir={'cvangysel': 'py/cvangysel'},
      python_requires='>=3',
      url='https://github.com/cvangysel/cvangysel-common',
      download_url='https://github.com/cvangysel/cvangysel-common/tarball/0.2',
      keywords=['tools', 'reusable'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python',
          'Intended Audience :: Science/Research',
          'Operating System :: POSIX :: Linux',
      ])
