from setuptools import setup

setup(
    name='autochektools',
    version='0.0.1',
    description='Autocheck data engineering private utilities package.',
    url='git@github.com:Autochek-Africa/autochek-de-tools.git',
    author='Vincent Omondi',
    author_email='vincent.o@autochek.africa',
    license='unlicense',
    install_requires=[
        "numpy", "google-api-core", "google-auth", "google-cloud-core", 
        "google-cloud-storage", "google-auth-oauthlib"],
    packages=['autochektools'],
    zip_safe=False
)
