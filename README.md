# vertica-php-adapter

Simple PHP Db adapter that allows to communicate to HP Vertica databases. Implements Db communication via odbc_* functions.
Pure ODBC solution is not that good and modern as PDO but we have to use it due to the known PDO bug (https://bugs.php.net/bug.php?id=63949) that makes it impossible to fetch data from Vertica tables with VARCHAR columns longer than 255.

Requires:
* php >= 5.4
* php_odbc extension
* Vertica drivers

# Vertica Driver & ODBC layer installation (Linux systems)

Please make sure you have installed:
* php5-odbc
* odbcinst
* unixODBC

Make ODBC and Vertica drivers to work together:
* Download and extract Vertica drivers from official website https://my.vertica.com/vertica-client-drivers/ (it should match your Vertica Db version)
* Extract driver under /opt/vertica/
* create/edit file: /etc/odbc.ini (see example under vertica-php-adapter/examples/drivers/odbc.ini)
* create/edit file: /etc/odbcinst.ini (see example under vertica-php-adapter/examples/drivers/odbcinst.ini)
* create/edit file: /etc/vertica.ini (see example under vertica-php-adapter/examples/drivers/odbcinst.ini)
