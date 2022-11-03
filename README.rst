==============================================
OOB - Object Oriented Boto3 Library
==============================================

OOB is a SDK on top of amazon `boto3`_. This development kit allows Python developers to write object oriented code 
that makes use of objects to interact with AWS services. Documentation at our `doc site`_, including a list of
services that are supported.

.. _`boto3`: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
.. _`doc site`: https://jira

Quick Start
-----------

First, configure your amazon credentials with AWS account informations (in e.g. ``~/.aws/credentials``):

Development
-----------

Getting Started
~~~~~~~~~~~~~~~
Assuming that you have python3 and ``tox`` installed, set up your
environment and install the required dependencies like this instead of
the ``pip install`` defined above:

.. code-block:: sh

    $ tox -e py3
    $ source .tox/py3/activate

Running Tests
~~~~~~~~~~~~~
You can run tests in all supported Python versions using ``tox``. By default,
it will run all of the unit and functional tests, but you can also specify run individual tests with or without coverage.

.. code-block:: sh

    $ tox
    $ tox -e test -- tests/unit/awslambda/test_event.py
    $ tox -e test -- --cov=src/oob/awslambda tests/unit/awslambda

Submitting Code
~~~~~~~~~~~~~~~
Before submitting your code make sure to run ``black`` code formatter using this command:
.. code-block:: sh

    $ tox -e codestyle

And your test coverage is above 80%.

.. code-block:: sh

    $ tox -e tests

Submitting a release
~~~~~~~~~~~~~~~~~~~~
Use ``zest.releaser`` package to manage verions, changelog, and releases.

.. code-block:: sh

    $ pip install zest.releaser
    $ fullrelease

Generating Documentation
~~~~~~~~~~~~~~~~~~~~~~~~
Sphinx is used for documentation. You can generate HTML locally with the
following:

.. code-block:: sh

    $ tox -e docs