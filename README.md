# practice_delta_table

This repository contains scripts to practice the basics of using Delta tables with Apache Spark.

## Setup

### Install Packages

Ensure you have the following packages installed:

- [Java](https://www.oracle.com/java/technologies/javase-downloads.html)
- [Apache Spark](https://spark.apache.org/downloads.html)
- [Delta Lake](https://delta.io/)

### Create a Virtual Environment

To create a virtual environment, run the following command:

```sh
python -m venv .venv
```

### Activate virtual environment

Windows:
```sh
.venv\Scripts\activate
```

macOS and Linux:
```sh
source .venv/bin/activate
```

### Install Python Dependencies

Once the virtual environment is activated, install the necessary Python packages:
```sh
pip install pyspark delta-spark
```

## Usage

Run the scripts in this repository to practice using Delta tables with Spark. Ensure the virtual environment is activated before running any scripts.  
*This version assumes that you're using Spark in standalone mode, which should be sufficient for practicing with Delta tables.*
