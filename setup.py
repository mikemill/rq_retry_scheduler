from setuptools import setup, find_packages

try:
    import pypandoc

    def long_description():
        return pypandoc.convert_file('README.md', 'rst')

except ImportError:
    def long_description():
        return ''


setup(
    name='rq-retry-scheduler',
    version='0.2.0',
    url='https://github.com/mikemill/rq_retry_scheduler',
    description='RQ Retry and Scheduler',
    long_description=long_description(),
    author='Michael Miller',
    author_email='mikemill@gmail.com',
    packages=find_packages(exclude=['*tests*']),
    license='MIT',
    install_requires=['rq>=0.13'],
    zip_safe=False,
    platforms='any',
    entry_points={
        'console_scripts': [
            'rqscheduler = rq_retry_scheduler.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
