from setuptools import setup, find_packages


setup(
    name='rq-retry-scheduler',
    version='0.1.0a',
    url='https://github.com/mikemill/rq_retry_scheduler',
    description='RQ Retry and Scheduler',
    long_description=__doc__,
    author='Michael Miller',
    author_email='mikemill@gmail.com',
    packages=find_packages(exclude=['*tests*']),
    license='MIT',
    install_requires=['rq>=0.6.0'],
    zip_safe=False,
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
    ]
)
