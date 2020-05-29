from setuptools import setup

setup(
    name='doccano-admin-client',
    version='1.0',
    description='Python client for Doccano django admin',
    license='MIT',
    packages=['doccano_admin_client'],
    install_requires=[
        'django-admin-client'
    ],
    zip_safe=True,
)
